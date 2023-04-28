use super::PipelineDescr;
use super::{
    storage::Storage, AttachedConnector, ConfigDescr, ConfigId, ConnectorDescr, ConnectorId,
    ConnectorType, PipelineId, ProjectDB, ProjectDescr, ProjectId, ProjectStatus, Version,
};
use crate::db::{pg_setup, DBError};
use anyhow::Result as AnyResult;
use async_trait::async_trait;
use chrono::DateTime;
use pg_embed::postgres::PgEmbed;
use pretty_assertions::assert_eq;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use proptest_derive::Arbitrary;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::SystemTime;
use tempfile::TempDir;
use tokio::sync::Mutex;

struct DbHandle {
    db: ProjectDB,
    pg: PgEmbed,
    _temp_dir: TempDir,
}

impl Drop for DbHandle {
    fn drop(&mut self) {
        // We drop `pg` before the temp dir gets deleted (which will
        // shutdown postgres). Otherwise postgres log an error that the
        // directory is already gone during shutdown which could be
        // confusing for a developer.
        let _r = async {
            self.pg.stop_db().await.unwrap();
        };
    }
}

async fn test_setup() -> DbHandle {
    let _temp_dir = tempfile::tempdir().unwrap();
    let temp_path = _temp_dir.path();

    use std::net::TcpListener;
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
    let pg = pg_setup::install(temp_path.into(), false, Some(port))
        .await
        .unwrap();

    let conn = ProjectDB::connect_inner(&pg.db_uri, &Some("".to_string()))
        .await
        .unwrap();

    DbHandle {
        db: conn,
        pg: pg,
        _temp_dir,
    }
}

#[tokio::test]
async fn project_creation() {
    let handle = test_setup().await;
    let res = handle
        .db
        .new_project("test1", "project desc", "ignored")
        .await
        .unwrap();
    let rows = handle.db.list_projects().await.unwrap();
    assert_eq!(1, rows.len());
    let expected = ProjectDescr {
        project_id: res.0,
        name: "test1".to_string(),
        description: "project desc".to_string(),
        version: res.1,
        status: ProjectStatus::None,
        schema: None,
    };
    let actual = rows.get(0).unwrap();
    assert_eq!(&expected, actual);
}

#[tokio::test]
async fn duplicate_project() {
    let handle = test_setup().await;
    let _ = handle
        .db
        .new_project("test1", "project desc", "ignored")
        .await;
    let res = handle
        .db
        .new_project("test1", "project desc", "ignored")
        .await
        .expect_err("Expecting unique violation");
    let expected = anyhow::anyhow!(DBError::DuplicateProjectName("test1".to_string()));
    assert_eq!(format!("{}", res), format!("{}", expected));
}

#[tokio::test]
async fn project_reset() {
    let handle = test_setup().await;
    handle
        .db
        .new_project("test1", "project desc", "ignored")
        .await
        .unwrap();
    handle
        .db
        .new_project("test2", "project desc", "ignored")
        .await
        .unwrap();
    handle.db.reset_project_status().await.unwrap();
    let results = handle.db.list_projects().await.unwrap();
    for p in results {
        assert_eq!(ProjectStatus::None, p.status);
        assert_eq!(None, p.schema); //can't check for error fields directly
    }
    let results = handle
        .db
        .conn
        .query(
            "SELECT * FROM project WHERE status != '' OR error != '' OR schema != ''",
            &[],
        )
        .await;
    assert_eq!(0, results.unwrap().len());
}

#[tokio::test]
async fn project_code() {
    let handle = test_setup().await;
    let (project_id, _) = handle
        .db
        .new_project("test1", "project desc", "create table t1(c1 integer);")
        .await
        .unwrap();
    let results = handle.db.project_code(project_id).await.unwrap();
    assert_eq!("test1", results.0.name);
    assert_eq!("project desc", results.0.description);
    assert_eq!("create table t1(c1 integer);".to_owned(), results.1);
}

#[tokio::test]
async fn update_project() {
    let handle = test_setup().await;
    let (project_id, _) = handle
        .db
        .new_project("test1", "project desc", "create table t1(c1 integer);")
        .await
        .unwrap();
    let _ = handle
        .db
        .update_project(project_id, "updated_test1", "some new description", &None)
        .await;
    let results = handle.db.list_projects().await.unwrap();
    assert_eq!(1, results.len());
    let row = results.get(0).unwrap();
    assert_eq!("updated_test1", row.name);
    assert_eq!("some new description", row.description);
}

#[tokio::test]
async fn project_queries() {
    let handle = test_setup().await;
    let (project_id, _) = handle
        .db
        .new_project("test1", "project desc", "create table t1(c1 integer);")
        .await
        .unwrap();
    let desc = handle
        .db
        .get_project_if_exists(project_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!("test1", desc.name);
    let desc = handle.db.lookup_project("test1").await.unwrap().unwrap();
    assert_eq!("test1", desc.name);
    let desc = handle.db.lookup_project("test2").await.unwrap();
    assert!(desc.is_none());
}

#[tokio::test]
async fn update_status() {
    let handle = test_setup().await;
    let (project_id, _) = handle
        .db
        .new_project("test1", "project desc", "create table t1(c1 integer);")
        .await
        .unwrap();
    let desc = handle
        .db
        .get_project_if_exists(project_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ProjectStatus::None, desc.status);
    handle
        .db
        .set_project_status(project_id, ProjectStatus::CompilingRust)
        .await
        .unwrap();
    let desc = handle
        .db
        .get_project_if_exists(project_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ProjectStatus::CompilingRust, desc.status);
}

/// Actions we can do on the Storage trait.
#[derive(Debug, Clone, Arbitrary)]
enum StorageAction {
    ResetProjectStatus,
    ListProjects,
    ProjectCode(ProjectId),
    NewProject(String, String, String),
    UpdateProject(ProjectId, String, String, Option<String>),
    GetProjectIfExists(ProjectId),
    LookupProject(String),
    SetProjectStatus(ProjectId, ProjectStatus),
    SetProjectStatusGuarded(ProjectId, Version, ProjectStatus),
    SetProjectSchema(ProjectId, String),
    DeleteProject(ProjectId),
    NextJob,
    ListConfigs,
    GetConfig(ConfigId),
    NewConfig(
        Option<ProjectId>,
        String,
        String,
        String,
        Option<Vec<AttachedConnector>>,
    ),
    AddPipelineToConfig(ConfigId, PipelineId),
    RemovePipelineFromConfig(ConfigId),
    UpdateConfig(
        ConfigId,
        Option<ProjectId>,
        String,
        String,
        Option<String>,
        Option<Vec<AttachedConnector>>,
    ),
    DeleteConfig(ConfigId),
    NewPipeline(ConfigId, Version),
    PipelineSetPort(PipelineId, u16),
    SetPipelineKilled(PipelineId),
    DeletePipeline(PipelineId),
    GetPipeline(PipelineId),
    ListPipelines,
    NewConnector(String, String, ConnectorType, String),
    ListConnectors,
    GetConnector(ConnectorId),
    UpdateConnector(ConnectorId, String, String, Option<String>),
    DeleteConnector(ConnectorId),
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

// Compare everything except the `created` field which gets set inside the DB.
fn compare_pipeline(step: usize, model: AnyResult<PipelineDescr>, impl_: AnyResult<PipelineDescr>) {
    match (model, impl_) {
        (Ok(mut mr), Ok(mut ir)) => {
            mr.created = DateTime::default();
            ir.created = DateTime::default();
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
            .map(|p| p.created = DateTime::default())
            .collect::<Vec<_>>(),
        impl_response
            .iter_mut()
            .map(|p| p.created = DateTime::default())
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
    let mut runner = TestRunner::new(config);
    let res = runner
        .run(
            &prop::collection::vec(any::<StorageAction>(), 0..100),
            |actions| {
                let model = Mutex::new(DbModel::default());
                runtime.block_on(async {
                    // We empty all tables in the database before each test
                    // (with TRUNCATE TABLE). We also reset the sequence ids
                    // (with RESTART IDENTITY)
                    handle
                        .db
                        .conn
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
                            StorageAction::ResetProjectStatus => {
                                let model_response = model.reset_project_status().await;
                                let impl_response = handle.db.reset_project_status().await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::ListProjects => {
                                let model_response = model.list_projects().await.unwrap();
                                let mut impl_response = handle.db.list_projects().await.unwrap();
                                // Impl does not guarantee order of rows returned by SELECT
                                impl_response.sort_by(|a, b| a.project_id.cmp(&b.project_id));
                                assert_eq!(model_response, impl_response);
                            }
                            StorageAction::ProjectCode(project_id) => {
                                let model_response = model.project_code(project_id).await;
                                let impl_response = handle.db.project_code(project_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::NewProject(name, description, code) => {
                                let model_response =
                                    model.new_project(&name, &description, &code).await;
                                let impl_response =
                                    handle.db.new_project(&name, &description, &code).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::UpdateProject(project_id, name, description, code) => {
                                let model_response = model
                                    .update_project(project_id, &name, &description, &code)
                                    .await;
                                let impl_response = handle
                                    .db
                                    .update_project(project_id, &name, &description, &code)
                                    .await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetProjectIfExists(project_id) => {
                                let model_response = model.get_project_if_exists(project_id).await;
                                let impl_response =
                                    handle.db.get_project_if_exists(project_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::LookupProject(name) => {
                                let model_response = model.lookup_project(&name).await;
                                let impl_response = handle.db.lookup_project(&name).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::SetProjectStatus(project_id, status) => {
                                let model_response =
                                    model.set_project_status(project_id, status.clone()).await;
                                let impl_response =
                                    handle.db.set_project_status(project_id, status.clone()).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::SetProjectStatusGuarded(project_id, version, status) => {
                                let model_response = model
                                    .set_project_status_guarded(project_id, version, status.clone())
                                    .await;
                                let impl_response = handle
                                    .db
                                    .set_project_status_guarded(project_id, version, status.clone())
                                    .await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::SetProjectSchema(project_id, schema) => {
                                let model_response =
                                    model.set_project_schema(project_id, schema.clone()).await;
                                let impl_response =
                                    handle.db.set_project_schema(project_id, schema).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::DeleteProject(project_id) => {
                                let model_response = model.delete_project(project_id).await;
                                let impl_response = handle.db.delete_project(project_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::NextJob => {
                                let model_response = model.next_job().await;
                                let impl_response = handle.db.next_job().await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetConfig(config_id) => {
                                let model_response = model.get_config(config_id).await;
                                let impl_response = handle.db.get_config(config_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::ListConfigs => {
                                let model_response = model.list_configs().await.unwrap();
                                let mut impl_response = handle.db.list_configs().await.unwrap();
                                // Impl does not guarantee order of rows returned by SELECT
                                impl_response.sort_by(|a, b| a.config_id.cmp(&b.config_id));
                                assert_eq!(model_response, impl_response);
                            }
                            StorageAction::NewConfig(project_id, name, description, config, connectors) => {
                                let model_response =
                                    model.new_config(project_id, &name, &description, &config, &connectors.clone()).await;
                                let impl_response =
                                    handle.db.new_config(project_id, &name, &description, &config, &connectors).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::AddPipelineToConfig(config_id, pipeline_id) => {
                                let model_response = model.add_pipeline_to_config(config_id, pipeline_id).await;
                                let impl_response = handle.db.add_pipeline_to_config(config_id, pipeline_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::RemovePipelineFromConfig(config_id) => {
                                let model_response = model.remove_pipeline_from_config(config_id).await;
                                let impl_response = handle.db.remove_pipeline_from_config(config_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::UpdateConfig(config_id, project_id, name, description, config, connectors) => {
                                let model_response = model
                                    .update_config(config_id, project_id, &name, &description, &config, &connectors.clone())
                                    .await;
                                let impl_response = handle
                                    .db
                                    .update_config(config_id, project_id, &name, &description, &config, &connectors)
                                    .await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::DeleteConfig(config_id) => {
                                let model_response = model.delete_config(config_id).await;
                                let impl_response = handle.db.delete_config(config_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::NewPipeline(config_id, expected_version) => {
                                let model_response = model.new_pipeline(config_id, expected_version).await;
                                let impl_response = handle.db.new_pipeline(config_id, expected_version).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::PipelineSetPort(pipeline_id, port) => {
                                let model_response = model.pipeline_set_port(pipeline_id, port).await;
                                let impl_response = handle.db.pipeline_set_port(pipeline_id, port).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::SetPipelineKilled(pipeline_id) => {
                                let model_response = model.set_pipeline_killed(pipeline_id).await;
                                let impl_response = handle.db.set_pipeline_killed(pipeline_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::DeletePipeline(pipeline_id) => {
                                let model_response = model.delete_pipeline(pipeline_id).await;
                                let impl_response = handle.db.delete_pipeline(pipeline_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetPipeline(pipeline_id) => {
                                let model_response = model.get_pipeline(pipeline_id).await;
                                let impl_response = handle.db.get_pipeline(pipeline_id).await;
                                compare_pipeline(i, model_response, impl_response);
                            }
                            StorageAction::ListPipelines => {
                                let model_response = model.list_pipelines().await.unwrap();
                                let mut impl_response = handle.db.list_pipelines().await.unwrap();
                                // Impl does not guarantee order of rows returned by SELECT
                                impl_response.sort_by(|a, b| a.pipeline_id.cmp(&b.pipeline_id));
                                compare_pipelines(model_response, impl_response);
                            }
                            StorageAction::ListConnectors => {
                                let model_response = model.list_connectors().await.unwrap();
                                let mut impl_response = handle.db.list_connectors().await.unwrap();
                                // Impl does not guarantee order of rows returned by SELECT
                                impl_response.sort_by(|a, b| a.connector_id.cmp(&b.connector_id));
                                assert_eq!(model_response, impl_response);
                            }
                            StorageAction::NewConnector(name, description, typ, config) => {
                                let model_response =
                                    model.new_connector(&name, &description, typ, &config).await;
                                let impl_response =
                                    handle.db.new_connector(&name, &description, typ, &config).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetConnector(connector_id) => {
                                let model_response = model.get_connector(connector_id).await;
                                let impl_response = handle.db.get_connector(connector_id).await;
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
    pub next_project_id: i64,
    pub next_config_id: i64,
    pub next_connector_id: i64,
    pub next_pipeline_id: i64,

    // `projects` Format is: (project, code, created)
    pub projects: BTreeMap<ProjectId, (ProjectDescr, String, SystemTime)>,
    pub configs: BTreeMap<ConfigId, ConfigDescr>,
    pub connectors: BTreeMap<ConnectorId, ConnectorDescr>,
    pub pipelines: BTreeMap<PipelineId, PipelineDescr>,
}

#[async_trait]
impl Storage for Mutex<DbModel> {
    async fn reset_project_status(&self) -> anyhow::Result<()> {
        self.lock()
            .await
            .projects
            .values_mut()
            .for_each(|(p, _, _e)| {
                p.status = ProjectStatus::None;
                p.schema = None;
            });

        Ok(())
    }

    async fn list_projects(&self) -> anyhow::Result<Vec<ProjectDescr>> {
        Ok(self
            .lock()
            .await
            .projects
            .values()
            .map(|(p, _, _)| p.clone())
            .collect())
    }

    async fn project_code(
        &self,
        project_id: super::ProjectId,
    ) -> anyhow::Result<(ProjectDescr, String)> {
        self.lock()
            .await
            .projects
            .get(&project_id)
            .map(|(p, c, _e)| (p.clone(), c.clone()))
            .ok_or(anyhow::anyhow!(DBError::UnknownProject(project_id)))
    }

    async fn new_project(
        &self,
        project_name: &str,
        project_description: &str,
        project_code: &str,
    ) -> anyhow::Result<(super::ProjectId, super::Version)> {
        let mut s = self.lock().await;
        // This is a bit strange: PostgreSQL does increment the primary key even
        // if the insert fails due to duplicate name conflict.
        s.next_project_id += 1;

        if s.projects.values().any(|(p, _, _)| p.name == project_name) {
            return Err(anyhow::anyhow!(DBError::DuplicateProjectName(
                project_name.to_string()
            )));
        }

        let project_id = ProjectId(s.next_project_id);
        let version = Version(1);

        s.projects.insert(
            project_id,
            (
                ProjectDescr {
                    project_id,
                    name: project_name.to_owned(),
                    description: project_description.to_owned(),
                    status: ProjectStatus::None,
                    schema: None,
                    version,
                },
                project_code.to_owned(),
                SystemTime::now(),
            ),
        );

        Ok((project_id, version))
    }

    async fn update_project(
        &self,
        project_id: super::ProjectId,
        project_name: &str,
        project_description: &str,
        project_code: &Option<String>,
    ) -> anyhow::Result<super::Version> {
        let mut s = self.lock().await;
        if !s.projects.contains_key(&project_id) {
            return Err(anyhow::anyhow!(DBError::UnknownProject(project_id)));
        }

        if s.projects
            .values()
            .any(|(p, _, _)| p.name == project_name && p.project_id != project_id)
        {
            return Err(anyhow::anyhow!(DBError::DuplicateProjectName(
                project_name.to_string()
            )));
        }

        s.projects
            .get_mut(&project_id)
            .map(|(p, cur_code, _e)| {
                p.name = project_name.to_owned();
                p.description = project_description.to_owned();
                if let Some(code) = project_code {
                    if code != cur_code {
                        *cur_code = code.to_owned();
                        p.version.0 += 1;
                        p.schema = None;
                        p.status = ProjectStatus::None;
                    }
                }
                p.version
            })
            .ok_or(anyhow::anyhow!(DBError::UnknownProject(project_id)))
    }

    async fn get_project_if_exists(
        &self,
        project_id: super::ProjectId,
    ) -> anyhow::Result<Option<ProjectDescr>> {
        Ok(self
            .lock()
            .await
            .projects
            .get(&project_id)
            .map(|(p, _, _)| p.clone()))
    }

    async fn lookup_project(&self, project_name: &str) -> anyhow::Result<Option<ProjectDescr>> {
        Ok(self
            .lock()
            .await
            .projects
            .values()
            .find(|(p, _, _)| p.name == project_name)
            .map(|(p, _, _)| p.clone()))
    }

    async fn set_project_status(
        &self,
        project_id: super::ProjectId,
        status: ProjectStatus,
    ) -> anyhow::Result<()> {
        let _r = self
            .lock()
            .await
            .projects
            .get_mut(&project_id)
            .map(|(p, _, t)| {
                p.status = status;
                *t = SystemTime::now();
                // TODO: It's a bit odd that this function also resets the schema
                p.schema = Some("".to_string());
            });

        Ok(())
    }

    async fn set_project_status_guarded(
        &self,
        project_id: super::ProjectId,
        expected_version: super::Version,
        status: ProjectStatus,
    ) -> anyhow::Result<()> {
        self.lock()
            .await
            .projects
            .get_mut(&project_id)
            .map(|(p, _, t)| {
                if p.version == expected_version {
                    p.status = status;
                    *t = SystemTime::now();
                }
            })
            .ok_or(anyhow::anyhow!(DBError::UnknownProject(project_id)))
    }

    async fn set_project_schema(
        &self,
        project_id: super::ProjectId,
        schema: String,
    ) -> anyhow::Result<()> {
        let _r = self
            .lock()
            .await
            .projects
            .get_mut(&project_id)
            .map(|(p, _, _)| {
                p.schema = Some(schema);
            });

        Ok(())
    }

    async fn delete_project(&self, project_id: super::ProjectId) -> anyhow::Result<()> {
        let mut s = self.lock().await;

        s.projects
            .remove(&project_id)
            .map(|_| ())
            .ok_or(anyhow::anyhow!(DBError::UnknownProject(project_id)))?;
        // Foreign key delete:
        s.configs.retain(|_, c| c.project_id != Some(project_id));

        Ok(())
    }

    async fn next_job(&self) -> anyhow::Result<Option<(super::ProjectId, super::Version)>> {
        let s = self.lock().await;
        let mut values = Vec::from_iter(s.projects.values());
        values.sort_by(|(_, _, t1), (_, _, t2)| t1.cmp(t2));

        values
            .iter()
            .find(|(p, _, _)| p.status == ProjectStatus::Pending)
            .map(|(p, _, _)| Ok(Some((p.project_id, p.version))))
            .unwrap_or(Ok(None))
    }

    async fn list_configs(&self) -> anyhow::Result<Vec<ConfigDescr>> {
        Ok(self.lock().await.configs.values().cloned().collect())
    }

    async fn get_config(&self, config_id: super::ConfigId) -> anyhow::Result<ConfigDescr> {
        self.lock()
            .await
            .configs
            .get(&config_id)
            .cloned()
            .ok_or(anyhow::anyhow!(DBError::UnknownConfig(config_id)))
    }

    async fn new_config(
        &self,
        project_id: Option<super::ProjectId>,
        config_name: &str,
        config_description: &str,
        config: &str,
        // TODO: not clear why connectors is an option here
        connectors: &Option<Vec<AttachedConnector>>,
    ) -> anyhow::Result<(super::ConfigId, super::Version)> {
        let mut s = self.lock().await;
        s.next_config_id += 1;

        // Model the foreign key constraint on `project_id`
        if let Some(project_id) = project_id {
            if !s.projects.contains_key(&project_id) {
                return Err(anyhow::anyhow!(DBError::UnknownProject(project_id)));
            }
        }

        // lel transactions
        let config_id = ConfigId(s.next_config_id);
        let version = Version(1);

        s.configs.insert(
            config_id,
            ConfigDescr {
                config_id,
                project_id,
                pipeline: None,
                name: config_name.to_owned(),
                description: config_description.to_owned(),
                config: config.to_owned(),
                attached_connectors: Vec::new(),
                version: Version(1),
            },
        );

        // TODO: The db does currently not use transactions; so we mimic the
        // same behavior as the current implementation which means we insert
        // attached_connectors until we encouter one that doesn't exist
        if let Some(connectors) = connectors {
            for ac in connectors {
                if s.connectors.contains_key(&ac.connector_id) {
                    s.configs
                        .get_mut(&config_id)
                        .unwrap() // we just inserted
                        .attached_connectors
                        .push(ac.clone());
                } else {
                    return Err(anyhow::anyhow!(DBError::UnknownConnector(ac.connector_id)));
                }
            }
        }

        Ok((config_id, version))
    }

    async fn add_pipeline_to_config(
        &self,
        config_id: super::ConfigId,
        pipeline_id: super::PipelineId,
    ) -> anyhow::Result<()> {
        let mut s = self.lock().await;
        if !s.configs.contains_key(&config_id) {
            return Err(anyhow::anyhow!(DBError::UnknownConfig(config_id)));
        }
        if let Some(pipeline) = s.pipelines.get(&pipeline_id) {
            s.configs
                .get_mut(&config_id)
                .unwrap() // we just checked
                .pipeline = Some(pipeline.clone());
        } else {
            return Err(anyhow::anyhow!(DBError::UnknownPipeline(pipeline_id)));
        }

        Ok(())
    }

    async fn remove_pipeline_from_config(&self, config_id: super::ConfigId) -> anyhow::Result<()> {
        let mut s = self.lock().await;
        if let Some(mut config) = s.configs.get_mut(&config_id) {
            config.pipeline = None;
            Ok(())
        } else {
            return Err(anyhow::anyhow!(DBError::UnknownConfig(config_id)));
        }
    }

    async fn update_config(
        &self,
        config_id: ConfigId,
        project_id: Option<ProjectId>,
        config_name: &str,
        config_description: &str,
        config: &Option<String>,
        connectors: &Option<Vec<AttachedConnector>>,
    ) -> anyhow::Result<Version> {
        // config must exist
        let mut s = self.lock().await;
        if !s.configs.contains_key(&config_id) {
            return Err(anyhow::anyhow!(DBError::UnknownConfig(config_id)));
        }
        let db_connectors = s.connectors.clone();
        let db_projects = s.projects.clone();

        let mut c = s
            .configs
            .get_mut(&config_id)
            .ok_or(anyhow::anyhow!(DBError::UnknownConfig(config_id)))?;

        // TODO: The db does currently not use transactions; so we mimic the
        // same behavior as the current implementation which means we insert
        // attached_connectors until we encouter one that doesn't exist
        if let Some(connectors) = connectors {
            c.attached_connectors.clear();
            for ac in connectors {
                if db_connectors.contains_key(&ac.connector_id) {
                    c.attached_connectors.push(ac.clone());
                } else {
                    return Err(anyhow::anyhow!(DBError::UnknownConnector(ac.connector_id)));
                }
            }
        }
        // Foreign key constraint on `project_id`
        if let Some(project_id) = project_id {
            if db_projects.contains_key(&project_id) {
                c.project_id = Some(project_id);
            } else {
                return Err(anyhow::anyhow!(DBError::UnknownProject(project_id)));
            }
        } else {
            c.project_id = None;
        }

        c.name = config_name.to_owned();
        c.description = config_description.to_owned();
        c.version = c.version.increment();
        if let Some(config) = config {
            c.config = config.clone();
        }

        Ok(c.version)
    }

    async fn delete_config(&self, config_id: super::ConfigId) -> anyhow::Result<()> {
        self.lock()
            .await
            .configs
            .remove(&config_id)
            .ok_or(anyhow::anyhow!(DBError::UnknownConfig(config_id)))?;

        Ok(())
    }

    async fn get_attached_connector_direction(
        &self,
        _uuid: &str,
    ) -> anyhow::Result<crate::Direction> {
        // TODO: This API doesn't make sense yet (uuid will change to name and/or
        // connector_id)
        todo!()
    }

    async fn new_pipeline(
        &self,
        config_id: ConfigId,
        _expected_config_version: Version,
    ) -> anyhow::Result<PipelineId> {
        let mut s = self.lock().await;
        s.next_pipeline_id += 1;
        let pipeline_id = PipelineId(s.next_pipeline_id);

        // TODO: it's probably not ideal to have PipelineDescr twice, in config
        // and a separate BTreeMap so we always need to update both.
        let _config = s
            .configs
            .get(&config_id)
            .ok_or(anyhow::anyhow!(DBError::UnknownConfig(config_id)))?;

        s.pipelines.insert(
            pipeline_id,
            PipelineDescr {
                pipeline_id,
                config_id: Some(config_id),
                port: 0,
                killed: false,
                created: DateTime::default(),
            },
        );

        Ok(pipeline_id)
    }

    async fn pipeline_set_port(
        &self,
        pipeline_id: super::PipelineId,
        port: u16,
    ) -> anyhow::Result<()> {
        let mut s = self.lock().await;

        s.pipelines.get_mut(&pipeline_id).map(|p| p.port = port);
        s.configs.values_mut().for_each(|c| {
            if let Some(pipeline) = &mut c.pipeline {
                if pipeline.pipeline_id == pipeline_id {
                    pipeline.port = port;
                }
            }
        });

        Ok(())
    }

    async fn set_pipeline_killed(&self, pipeline_id: super::PipelineId) -> anyhow::Result<bool> {
        let mut s = self.lock().await;
        s.configs.values_mut().for_each(|c| {
            if let Some(pipeline) = &mut c.pipeline {
                if pipeline.pipeline_id == pipeline_id {
                    pipeline.killed = true;
                }
            }
        });

        Ok(s.pipelines
            .get_mut(&pipeline_id)
            .map(|p| {
                p.killed = true;
                p.killed
            })
            .unwrap_or(false))
    }

    async fn delete_pipeline(&self, pipeline_id: super::PipelineId) -> anyhow::Result<bool> {
        let mut s = self.lock().await;

        s.configs.values_mut().for_each(|c| {
            if let Some(pipeline) = &mut c.pipeline {
                if pipeline.pipeline_id == pipeline_id {
                    c.pipeline = None;
                }
            }
        });

        // TODO: Our APIs sometimes are not consistent we return a bool here but other
        // calls fail silently on delete/lookups
        Ok(s.pipelines
            .remove(&pipeline_id)
            .map(|_| true)
            .unwrap_or(false))
    }

    async fn get_pipeline(
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

    async fn list_pipelines(&self) -> anyhow::Result<Vec<super::PipelineDescr>> {
        Ok(self.lock().await.pipelines.values().cloned().collect())
    }

    async fn new_connector(
        &self,
        name: &str,
        description: &str,
        typ: super::ConnectorType,
        config: &str,
    ) -> anyhow::Result<super::ConnectorId> {
        let mut s = self.lock().await;
        s.next_connector_id += 1;
        let connector_id = super::ConnectorId(s.next_connector_id);
        s.connectors.insert(
            connector_id,
            ConnectorDescr {
                connector_id,
                name: name.to_owned(),
                description: description.to_owned(),
                direction: typ.into(),
                typ,
                config: config.to_owned(),
            },
        );
        Ok(connector_id)
    }

    async fn list_connectors(&self) -> anyhow::Result<Vec<ConnectorDescr>> {
        Ok(self.lock().await.connectors.values().cloned().collect())
    }

    async fn get_connector(
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

    async fn update_connector(
        &self,
        connector_id: super::ConnectorId,
        connector_name: &str,
        description: &str,
        config: &Option<String>,
    ) -> anyhow::Result<()> {
        let mut s = self.lock().await;
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
        s.configs.values_mut().for_each(|c| {
            c.attached_connectors
                .retain(|c| c.connector_id != connector_id);
        });
        Ok(())
    }
}
