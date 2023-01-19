//! HTTP server that manages cataloging, compilation,
//! and execution of SQL programs.
//!
//! # API concepts
//!
//! * Project.  A project is a SQL script with a non-unique name and a unique ID
//!   attached to it.  The client can add/remove/modify/compile a project.
//!   Compilation includes running the SQL-to-DBSP compiler followed by the Rust
//!   compiler.
//!
//! * Configuration.  A project can have multiple configurations associated with
//!   it.  A configuration specifies input and output streams as a YAML file
//!   that is deserialized into a `ControllerConfig` object.  Similar to
//!   projects, one can add/remove/modify configs.
//!
//! * Pipeline.  A pipeline is a running instance of a compiled project based on
//!   one of the configs.  One can start multiple pipelines for a project with
//!   the same or different configs.
//!
//! The API is currently single-tenant: there is no concept of users or
//! permissions.  Multi-tenancy can be implemented by creating a manager
//! instance per tenant, which enables better separation of concerns,
//! resource isolation and fault isolation compared to buiding multitenancy
//! into the manager.
//!
//! # Architecture
//!
//! * Project database.  Projects (including SQL source code), configs, and
//!   pipelines are stored in a Postgres database.  The database is the only
//!   state that is expected to survive across server restarts.  Intermediate
//!   artifacts stored in the file system (see below) can be safely deleted.
//!
//! * Compiler.  The compiler generates a binary crate for each project and adds
//!   it to a cargo workspace that also includes libraries that come with the
//!   SQL libraries.  This way, all precompiled dependencies of the main crate
//!   are reused across projects, thus speeding up compilation.
//!
//! * Runner.  The runner component is responsible for starting and killing
//!   compiled pipelines and for interacting with them at runtime.  It also
//!   registers each pipeline with Prometheus.
//!
//! # Concurrency
//!
//! We want to prevent race conditions due to users accessing the same project
//! from different browsers.  An example is user 1 modifying the project,
//! while user 2 is starting a pipeline for the same project.  The pipeline
//! may end up running the old or the new version.  Our very simple solution
//! is to increment project version on each update.  Every request to compile
//! the project or start a pipeline must include project id _and_ version
//! number. If the version number isn't equal to the current version in the
//! database, this means that the last version of the project observed by the
//! user is outdated, so the request is rejected.  Note that we don't store old
//! versions, only the latest one.

// TODOs:
// * Tests.
// * Generate a proper JSON API + docs using, e.g., OpenAPI.
// * Support multi-node DBSP deployments (the current architecture assumes that
//   pipelines and the Prometheus server run on the same host as this server).
// * Proper UI.

use actix_files as fs;
use actix_files::NamedFile;
use actix_web::{
    dev::{ServiceFactory, ServiceRequest},
    get,
    http::header::{CacheControl, CacheDirective},
    middleware::Logger,
    post, web,
    web::Data as WebData,
    App, Error as ActixError, HttpRequest, HttpResponse, HttpServer, Responder,
    Result as ActixResult,
};
use actix_web_static_files::ResourceFiles;
use anyhow::{Error as AnyError, Result as AnyResult};
use clap::Parser;
use env_logger::Env;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::{fs::read, sync::Mutex};

mod compiler;
mod config;
mod db;
mod runner;

pub(crate) use compiler::{Compiler, ProjectStatus};
pub(crate) use config::ManagerConfig;
use db::{ConfigId, DBError, PipelineId, ProjectDB, ProjectId, Version};
use runner::Runner;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Server configuration YAML file.
    #[arg(short, long)]
    config_file: Option<String>,

    /// [Developers only] serve static content from the specified directory.
    /// Allows modifying JavaScript without restarting the server.
    #[arg(short, long)]
    static_html: Option<String>,
}

#[actix_web::main]
async fn main() -> AnyResult<()> {
    // Create env logger.
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let args = Args::try_parse()?;

    let config_file = &args
        .config_file
        .unwrap_or_else(|| "config.yaml".to_string());
    let config_yaml = read(config_file)
        .await
        .map_err(|e| AnyError::msg(format!("error reading config file '{config_file}': {e}")))?;
    let config_yaml = String::from_utf8_lossy(&config_yaml);
    let mut config: ManagerConfig = serde_yaml::from_str(&config_yaml)
        .map_err(|e| AnyError::msg(format!("error parsing config file '{config_file}': {e}")))?;

    if let Some(static_html) = &args.static_html {
        config.static_html = Some(static_html.clone());
    }
    let config = config.canonicalize().await?;

    run(config).await
}

struct ServerState {
    // Serialize DB access with a lock, so we don't need to deal with
    // transaction conflicts.  The server must avoid holding this lock
    // for a long time to avoid blocking concurrent requests.
    db: Arc<Mutex<ProjectDB>>,
    // Dropping this handle kills the compiler task.
    _compiler: Compiler,
    runner: Runner,
    config: ManagerConfig,
}

impl ServerState {
    async fn new(
        config: ManagerConfig,
        db: Arc<Mutex<ProjectDB>>,
        compiler: Compiler,
    ) -> AnyResult<Self> {
        let runner = Runner::new(db.clone(), &config).await?;

        Ok(Self {
            db,
            _compiler: compiler,
            runner,
            config,
        })
    }
}

async fn run(config: ManagerConfig) -> AnyResult<()> {
    let db = Arc::new(Mutex::new(ProjectDB::connect(&config).await?));
    let compiler = Compiler::new(&config, db.clone()).await?;

    // Since we don't trust any file system state after restart,
    // reset all projects to `ProjectStatus::None`, which will force
    // us to recompile projects before running them.
    db.lock().await.reset_project_status().await?;

    let port = config.port;
    let state = WebData::new(ServerState::new(config, db, compiler).await?);

    HttpServer::new(move || build_app(App::new().wrap(Logger::default()), state.clone()))
        .bind(("127.0.0.1", port))?
        .run()
        .await?;

    Ok(())
}

// `static_files` magic.
include!(concat!(env!("OUT_DIR"), "/generated.rs"));

fn build_app<T>(app: App<T>, state: WebData<ServerState>) -> App<T>
where
    T: ServiceFactory<ServiceRequest, Config = (), Error = ActixError, InitError = ()>,
{
    // Creates a dictionary of static files indexed by file name.
    let generated = generate();

    // Extract the contents of `index.html`, so we can serve it on the
    // root endpoint (`/`), while other files are served via the`/static`
    // endpoint.
    let index_data = match generated.get("index.html") {
        None => "<html><head><title>DBSP manager</title></head></html>"
            .as_bytes()
            .to_owned(),
        Some(resource) => resource.data.to_owned(),
    };

    let app = app
        .app_data(state.clone())
        .service(list_projects)
        .service(project_code)
        .service(project_status)
        .service(new_project)
        .service(update_project)
        .service(compile_project)
        .service(delete_project)
        .service(new_config)
        .service(update_config)
        .service(delete_config)
        .service(list_project_configs)
        .service(new_pipeline)
        .service(kill_pipeline)
        .service(delete_pipeline)
        .service(list_project_pipelines);

    if let Some(static_html) = &state.config.static_html {
        // Serve static contents from the file system.
        app.route("/", web::get().to(index))
            .service(fs::Files::new("/static", static_html).show_files_listing())
    } else {
        // Serve static contents embedded in the program.
        app.route(
            "/",
            web::get().to(move || {
                let index_data = index_data.clone();
                async { HttpResponse::Ok().body(index_data) }
            }),
        )
        .service(ResourceFiles::new("/static", generated))
    }
}

async fn index() -> ActixResult<NamedFile> {
    Ok(NamedFile::open("static/index.html")?)
}

fn http_resp_from_error(error: &AnyError) -> HttpResponse {
    if let Some(db_error) = error.downcast_ref::<DBError>() {
        let msg = db_error.to_string();
        match db_error {
            DBError::UnknownProject(_) => HttpResponse::NotFound(),
            DBError::OutdatedProjectVersion(_) => HttpResponse::Conflict(),
            DBError::UnknownConfig(_) => HttpResponse::NotFound(),
            DBError::UnknownPipeline(_) => HttpResponse::NotFound(),
        }
        .body(msg)
    } else {
        HttpResponse::InternalServerError().body(error.to_string())
    }
}

/// Enumerate the project database.
///
/// Returns an array of [project descriptors](`db::ProjectDescr`).
#[get("/list_projects")]
async fn list_projects(state: WebData<ServerState>) -> impl Responder {
    state
        .db
        .lock()
        .await
        .list_projects()
        .await
        .map(|projects| {
            let json_string = serde_json::to_string(&projects).unwrap();
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        })
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// Response to a `/project_code` request.
#[derive(Serialize)]
struct ProjectCodeResponse {
    /// Current project version.
    version: Version,
    /// Project code.
    code: String,
}

/// Returns the latest SQL source code of the project.
///
/// # HTTP errors
///
/// * `BAD_REQUEST` - missing or invalid `project_id`.
/// * `NOT_FOUND` - `project_id` does not exist in the database.
#[get("/project_code/{project_id}")]
async fn project_code(state: WebData<ServerState>, req: HttpRequest) -> impl Responder {
    let project_id = match req.match_info().get("project_id") {
        None => {
            return HttpResponse::BadRequest().body("missing project id argument");
        }
        Some(project_id) => match project_id.parse::<ProjectId>() {
            Err(e) => {
                return HttpResponse::BadRequest()
                    .body(format!("invalid project id '{project_id}': {e}"));
            }
            Ok(project_id) => project_id,
        },
    };

    state
        .db
        .lock()
        .await
        .project_code(project_id)
        .await
        .map(|(version, code)| {
            let json_string =
                serde_json::to_string(&ProjectCodeResponse { version, code }).unwrap();
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        })
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// Response to a `/project_status` request.
#[derive(Serialize)]
struct ProjectStatusResponse {
    /// Current project version.
    version: Version,
    /// Project compilation status.
    status: ProjectStatus,
}

/// Returns current project version and compilation status.
///
/// # HTTP errors
///
/// * `BAD_REQUEST` - missing or invalid `project_id`.
/// * `NOT_FOUND` - `project_id` does not exist in the database.
#[get("/project_status/{project_id}")]
async fn project_status(state: WebData<ServerState>, req: HttpRequest) -> impl Responder {
    let project_id = match req.match_info().get("project_id") {
        None => {
            return HttpResponse::BadRequest().body("missing project id argument");
        }
        Some(project_id) => match project_id.parse::<ProjectId>() {
            Err(e) => {
                return HttpResponse::BadRequest()
                    .body(format!("invalid project id '{project_id}': {e}"));
            }
            Ok(project_id) => project_id,
        },
    };

    state
        .db
        .lock()
        .await
        .get_project(project_id)
        .await
        .map(|descr| {
            let json_string = serde_json::to_string(&ProjectStatusResponse {
                version: descr.version,
                status: descr.status,
            })
            .unwrap();
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        })
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// `/new_project` request parameters.
#[derive(Deserialize)]
struct NewProjectRequest {
    /// Project name.
    name: String,
    /// SQL code of the project.
    code: String,
}

/// Response to a `/new_project` request
#[derive(Serialize)]
struct NewProjectResponse {
    /// Id of the newly created project.
    project_id: ProjectId,
    /// Initial project version (this field is always set to 1).
    version: Version,
}

/// Create a new project.
#[post("/new_project")]
async fn new_project(
    state: WebData<ServerState>,
    request: web::Json<NewProjectRequest>,
) -> impl Responder {
    state
        .db
        .lock()
        .await
        .new_project(&request.name, &request.code)
        .await
        .map(|(project_id, version)| {
            let json_string = serde_json::to_string(&NewProjectResponse {
                project_id,
                version,
            })
            .unwrap();
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        })
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// `/update_project` request parameters.
#[derive(Deserialize)]
struct UpdateProjectRequest {
    /// Id of the project.
    project_id: ProjectId,
    /// New name for the project.
    name: String,
    /// New SQL code for the project or `None` to keep existing project
    /// code unmodified.
    code: Option<String>,
}

/// Response to a `/update_project` request.
#[derive(Serialize)]
struct UpdateProjectResponse {
    /// New project version.  Equals the previous version if project code
    /// doesn't change or previous version +1 if it does.
    version: Version,
}

/// Change project code and/or name.
///
/// If project code changes, any ongoing compilation gets cancelled,
/// project status is reset to `ProjectStatus::None`, and project version
/// is incremented by 1.  Changing project name only doesn't affect its
/// version or the compilation process.
///
/// # HTTP errors
///
/// * `NOT_FOUND` - `project_id` does not exist in the database.
#[post("/update_project")]
async fn update_project(
    state: WebData<ServerState>,
    request: web::Json<UpdateProjectRequest>,
) -> impl Responder {
    state
        .db
        .lock()
        .await
        .update_project(request.project_id, &request.name, &request.code)
        .await
        .map(|version| {
            let json_string = serde_json::to_string(&UpdateProjectResponse { version }).unwrap();
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        })
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// `/compile_project` request parameters.
#[derive(Deserialize)]
struct CompileProjectRequest {
    /// Project id.
    project_id: ProjectId,
    /// Latest project version known to the client.
    version: Version,
}

/// Queue project for compilation.
///
/// The client should poll the `/project_status` endpoint
/// for compilation results.
///
/// # HTTP errors
///
/// * `NOT_FOUND` - `project_id` does not exist in the database.
/// * `CONFLICT` - project version specified in the request doesn't match the
///   latest project version in the database.
#[post("/compile_project")]
async fn compile_project(
    state: WebData<ServerState>,
    request: web::Json<CompileProjectRequest>,
) -> impl Responder {
    state
        .db
        .lock()
        .await
        .set_project_pending(request.project_id, request.version)
        .await
        .map(|_| HttpResponse::Ok().finish())
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// `/cancel_project` request parameters.
#[derive(Deserialize)]
struct CancelProjectRequest {
    /// Project id.
    project_id: ProjectId,
    /// Latest project version known to the client.
    version: Version,
}

/// Cancel outstanding compilation request.
///
/// The client should poll the `/project_status` endpoint
/// to determine when the cancelation request completes.
///
/// # HTTP errors
///
/// * `NOT_FOUND` - `project_id` does not exist in the database.
///
/// * `CONFLICT` - project version specified in the request doesn't match the
///   latest project version in the database.
#[post("/cancel_project")]
async fn cancel_project(
    state: WebData<ServerState>,
    request: web::Json<CancelProjectRequest>,
) -> impl Responder {
    state
        .db
        .lock()
        .await
        .cancel_project(request.project_id, request.version)
        .await
        .map(|_| HttpResponse::Ok().finish())
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// `/delete_project` request parameters.
#[derive(Deserialize)]
struct DeleteProjectRequest {
    project_id: ProjectId,
}

/// Delete a project.
///
/// # HTTP errors
///
/// * `BAD_REQUEST` - the project has one or more running pipelines.
///
/// * `NOT_FOUND` - `project_id` does not exist in the database.
#[post("/delete_project")]
async fn delete_project(
    state: WebData<ServerState>,
    request: web::Json<DeleteProjectRequest>,
) -> impl Responder {
    let db = state.db.lock().await;

    match db.list_project_pipelines(request.project_id).await {
        Ok(pipelines) => {
            if pipelines.iter().any(|pipeline| !pipeline.killed) {
                return HttpResponse::BadRequest()
                    .body("Cannot delete a project while some of its pipelines are running");
            }
        }
        Err(e) => return http_resp_from_error(&e),
    }

    db.delete_project(request.project_id)
        .await
        .map(|_| HttpResponse::Ok().finish())
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// `/new_config` request parameters.
#[derive(Deserialize)]
struct NewConfigRequest {
    /// Project to create config for.
    project_id: ProjectId,
    /// Config name.
    name: String,
    /// YAML code for the config.
    config: String,
}

/// Response to a `/new_config` request.
#[derive(Serialize)]
struct NewConfigResponse {
    /// Id of the newly created config.
    config_id: ConfigId,
    /// Initial config version (this field is always set to 1).
    version: Version,
}

/// Create a new project configuration.
///
/// # HTTP errors
///
/// * `NOT_FOUND` - `project_id` does not exist in the database.
#[post("/new_config")]
async fn new_config(
    state: WebData<ServerState>,
    request: web::Json<NewConfigRequest>,
) -> impl Responder {
    state
        .db
        .lock()
        .await
        .new_config(request.project_id, &request.name, &request.config)
        .await
        .map(|(config_id, version)| {
            let json_string =
                serde_json::to_string(&NewConfigResponse { config_id, version }).unwrap();
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        })
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// `/update_config` request parameters.
#[derive(Deserialize)]
struct UpdateConfigRequest {
    /// Config id.
    config_id: ConfigId,
    /// New config name.
    name: String,
    /// New config YAML or `None` to keep existing YAML unmodified.
    config: Option<String>,
}

/// Response to an `/update_config` request.
#[derive(Serialize)]
struct UpdateConfigResponse {
    /// New config version.  Equals the previous version +1.
    version: Version,
}

/// Update existing project configuration.
///
/// Updates project config name and, optionally, code.
/// On success, increments config version by 1.
///
/// # HTTP errors
///
/// * `NOT_FOUND` - `config_id` does not exist in the database.
#[post("/update_config")]
async fn update_config(
    state: WebData<ServerState>,
    request: web::Json<UpdateConfigRequest>,
) -> impl Responder {
    state
        .db
        .lock()
        .await
        .update_config(request.config_id, &request.name, &request.config)
        .await
        .map(|version| {
            let json_string = serde_json::to_string(&UpdateConfigResponse { version }).unwrap();
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        })
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// `/delete_config` request parameters.
#[derive(Deserialize)]
struct DeleteConfigRequest {
    config_id: ConfigId,
}

/// Delete existing project configuration.
///
/// # HTTP errors
///
/// * `NOT_FOUND` - `config_id` does not exist in the database.
#[post("/delete_config")]
async fn delete_config(
    state: WebData<ServerState>,
    request: web::Json<DeleteConfigRequest>,
) -> impl Responder {
    state
        .db
        .lock()
        .await
        .delete_config(request.config_id)
        .await
        .map(|_| HttpResponse::Ok().finish())
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// `/list_project_configs` request parameters.
#[derive(Deserialize)]
struct ListProjectConfigsRequest {
    /// Project id to list configs for.
    project_id: ProjectId,
}

/// List project configs.
///
/// Returns an array of [configuration descriptors](`db::ConfigDescr`).
///
/// # HTTP errors
///
/// * `NOT_FOUND` - `project_id` does not exist in the database.
#[post("/list_project_configs")]
async fn list_project_configs(
    state: WebData<ServerState>,
    request: web::Json<ListProjectConfigsRequest>,
) -> impl Responder {
    state
        .db
        .lock()
        .await
        .list_project_configs(request.project_id)
        .await
        .map(|configs| {
            let json_string = serde_json::to_string(&configs).unwrap();
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        })
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// `/new_pipeline` request parameters.
#[derive(Deserialize)]
pub(self) struct NewPipelineRequest {
    /// Project id to create pipeline for.
    project_id: ProjectId,
    /// Latest project version known to the client.
    project_version: Version,
    /// Project config to run the pipeline with.
    config_id: ConfigId,
    /// Latest config version known to the client.
    config_version: Version,
}

/// Response to a `/new_pipeline` request.
#[derive(Serialize)]
struct NewPipelineResponse {
    /// Unique id assigned to the new pipeline.
    pipeline_id: PipelineId,
    /// TCP port that the pipeline process listens on.
    port: u16,
}

/// Launch a new pipeline.
///
/// Create a new pipeline for the specified project and config.
/// This is a synchronous endpoint, which sends a response once
/// the pipeline has been initialized.
///
/// # HTTP errors
///
/// * `NOT_FOUND` - `project_id` or `config_id` does not exist in the database.
///
/// * `CONFLICT` - project or config version in the request doesn't match the
/// latest version in the database.
///
/// * `BAD_REQUEST` - `config_id` refers to a config that does not belong to
/// `project_id`.
///
/// * `INTERNAL_SERVER_ERROR` - pipeline process failed to initialize.
#[post("/new_pipeline")]
async fn new_pipeline(
    state: WebData<ServerState>,
    request: web::Json<NewPipelineRequest>,
) -> impl Responder {
    state
        .runner
        .run_pipeline(&request)
        .await
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// `/list_project_pipelines` request parameters.
#[derive(Deserialize)]
struct ListProjectPipelinesRequest {
    /// Project id to list pipelines for.
    project_id: ProjectId,
}

/// List pipelines associated with a project.
///
/// Returns an array of [pipeline descriptors](`db::PipelineDescr`).
///
/// # HTTP errors
///
/// * `NOT_FOUND` - `project_id` does not exist in the database.
#[post("/list_project_pipelines")]
async fn list_project_pipelines(
    state: WebData<ServerState>,
    request: web::Json<ListProjectPipelinesRequest>,
) -> impl Responder {
    state
        .db
        .lock()
        .await
        .list_project_pipelines(request.project_id)
        .await
        .map(|pipelines| {
            let json_string = serde_json::to_string(&pipelines).unwrap();
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        })
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// `/kill_pipeline` request parameters.
#[derive(Deserialize)]
pub(self) struct KillPipelineRequest {
    /// Pipeline id to terminate.
    pipeline_id: PipelineId,
}

/// Terminate the execution of a pipeline.
///
/// Sends a termination request to the pipeline process.
/// Returns immediately, without waiting for the pipeline
/// to terminate (which can take several seconds).
///
/// The pipeline is not deleted from the database, but its
/// `killed` flag is set to `true`.
///
/// # HTTP errors
///
/// * `NOT_FOUND` - `pipeline_id` does not exist in the database.
///
/// * `INTERNAL_SERVER_ERROR` - pipeline process returned an HTTP
/// error in response to the termination command.
#[post("/kill_pipeline")]
async fn kill_pipeline(
    state: WebData<ServerState>,
    request: web::Json<KillPipelineRequest>,
) -> impl Responder {
    state
        .runner
        .kill_pipeline(request.pipeline_id)
        .await
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// `/delete_pipeline` request parameters.
#[derive(Deserialize)]
pub(self) struct DeletePipelineRequest {
    /// Pipeline id to delete.
    pipeline_id: PipelineId,
}

/// Terminate and delete a pipeline.
///
/// Kill the pipeline if it is still running and delete it from
/// the database.
///
/// # HTTP errors
///
/// * `NOT_FOUND` - `pipeline_id` does not exist in the database.
///
/// * `INTERNAL_SERVER_ERROR` - pipeline process returned an HTTP
/// error in response to the termination command.
#[post("/delete_pipeline")]
async fn delete_pipeline(
    state: WebData<ServerState>,
    request: web::Json<DeletePipelineRequest>,
) -> impl Responder {
    state
        .runner
        .delete_pipeline(request.pipeline_id)
        .await
        .unwrap_or_else(|e| http_resp_from_error(&e))
}
