//! DBSP Pipeline Manager provides an HTTP API to catalog, compile, and execute
//! SQL programs.
//!
//! The API is currently single-tenant: there is no concept of users or
//! permissions.  Multi-tenancy can be implemented by creating a manager
//! instance per tenant, which enables better separation of concerns,
//! resource isolation and fault isolation compared to buiding multitenancy
//! into the manager.
//!
//! # Architecture
//!
//! * Project database.  Programs (including SQL source code), configs, and
//!   pipelines are stored in a Postgres database.  The database is the only
//!   state that is expected to survive across server restarts.  Intermediate
//!   artifacts stored in the file system (see below) can be safely deleted.
//!
//! * Compiler.  The compiler generates a binary crate for each program and adds
//!   it to a cargo workspace that also includes libraries that come with the
//!   SQL libraries.  This way, all precompiled dependencies of the main crate
//!   are reused across programs, thus speeding up compilation.
//!
//! * Runner.  The runner component is responsible for starting and killing
//!   compiled pipelines and for interacting with them at runtime.

// TODOs:
// * Tests.
// * Support multi-node DBSP deployments.
// * Proper UI.

use actix_web::{
    delete,
    dev::{ServiceFactory, ServiceRequest},
    get,
    http::{
        header::{CacheControl, CacheDirective},
        Method,
    },
    middleware::{Condition, Logger},
    patch, post, rt, web,
    web::Data as WebData,
    App, Error as ActixError, HttpRequest, HttpResponse, HttpServer, Responder,
};
use actix_web_httpauth::middleware::HttpAuthentication;
use actix_web_static_files::ResourceFiles;
use anyhow::{Error as AnyError, Result as AnyResult};
use auth::JwkCache;
use clap::Parser;
use colored::Colorize;
#[cfg(unix)]
use daemonize::Daemonize;
use env_logger::Env;

use log::{debug, warn};
use serde::{Deserialize, Serialize};

use std::{env, io::Write};
use std::{
    fs::{read, write},
    net::TcpListener,
    sync::Arc,
};
use tokio::sync::Mutex;
use utoipa::{openapi::OpenApi as OpenApiDoc, OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;

mod auth;
mod compiler;
mod config;
mod db;
mod runner;

pub(crate) use compiler::{Compiler, ProgramStatus};
pub(crate) use config::ManagerConfig;
use db::{
    storage::Storage, AttachedConnector, AttachedConnectorId, ConnectorId, DBError, PipelineId,
    ProgramDescr, ProgramId, ProjectDB, Version,
};
use runner::{LocalRunner, Runner, RunnerError};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "DBSP API",
        description = r"API to catalog, compile, and execute SQL programs.

# API concepts

* *Program*.  A program is a SQL script with a unique name and a unique ID
  attached to it.  The client can add, remove, modify, and compile programs.
  Compilation includes running the SQL-to-DBSP compiler followed by the Rust
  compiler.

* *Configuration*.  A program can have multiple configurations associated with
  it.  Similar to programs, one can add, remove, and modify configs.

* *Pipeline*.  A pipeline is a running instance of a compiled program based on
  one of the configs.  Clients can start multiple pipelines for a program with
  the same or different configs.

# Concurrency

The API prevents race conditions due to multiple users accessing the same
program or configuration concurrently.  An example is user 1 modifying the program,
while user 2 is starting a pipeline for the same program.  The pipeline
may end up running the old or the new version, potentially leading to
unexpected behaviors.  The API prevents such situations by associating a
monotonically increasing version number with each program and configuration.
Every request to compile the program or start a pipeline must include program
id _and_ version number. If the version number isn't equal to the current
version in the database, this means that the last version of the program
observed by the user is outdated, so the request is rejected."
    ),
    paths(
        list_programs,
        program_code,
        program_status,
        new_program,
        update_program,
        compile_program,
        cancel_program,
        delete_program,
        new_pipeline,
        update_pipeline,
        list_pipelines,
        pipeline_stats,
        pipeline_status,
        pipeline_action,
        pipeline_delete,
        list_connectors,
        new_connector,
        update_connector,
        connector_status,
        delete_connector,
        http_input
    ),
    components(schemas(
        compiler::SqlCompilerMessage,
        db::AttachedConnector,
        db::ProgramDescr,
        db::ConnectorDescr,
        db::PipelineDescr,
        dbsp_adapters::PipelineConfig,
        dbsp_adapters::InputEndpointConfig,
        dbsp_adapters::OutputEndpointConfig,
        dbsp_adapters::TransportConfig,
        dbsp_adapters::FormatConfig,
        dbsp_adapters::transport::FileInputConfig,
        dbsp_adapters::transport::FileOutputConfig,
        dbsp_adapters::transport::KafkaInputConfig,
        dbsp_adapters::transport::KafkaOutputConfig,
        dbsp_adapters::transport::KafkaLogLevel,
        dbsp_adapters::transport::KafkaOutputConfig,
        dbsp_adapters::format::CsvEncoderConfig,
        dbsp_adapters::format::CsvParserConfig,
        ProgramId,
        PipelineId,
        ConnectorId,
        AttachedConnectorId,
        Version,
        ProgramStatus,
        ErrorResponse,
        ProgramCodeResponse,
        NewProgramRequest,
        NewProgramResponse,
        UpdateProgramRequest,
        UpdateProgramResponse,
        CompileProgramRequest,
        CancelProgramRequest,
        NewPipelineRequest,
        NewPipelineResponse,
        UpdatePipelineRequest,
        UpdatePipelineResponse,
        NewConnectorRequest,
        NewConnectorResponse,
        UpdateConnectorRequest,
        UpdateConnectorResponse,
    ),),
    tags(
        (name = "Program", description = "Manage programs"),
        (name = "Pipeline", description = "Manage pipelines"),
        (name = "Connector", description = "Manage data connectors"),
    ),
)]
pub struct ApiDoc;

fn main() -> AnyResult<()> {
    // Stay in single-threaded mode (no tokio) until calling `daemonize`.

    // Create env logger.
    let name = "[manager]".cyan();
    env_logger::Builder::from_env(Env::default().default_filter_or("debug"))
        .format(move |buf, record| {
            let t = chrono::Utc::now();
            let t = format!("{}", t.format("%Y-%m-%d %H:%M:%S"));
            writeln!(
                buf,
                "{} {} {} {}",
                t,
                buf.default_styled_level(record.level()),
                name,
                record.args()
            )
        })
        .init();

    let mut config = ManagerConfig::try_parse()?;

    if config.dump_openapi {
        let openapi_json = ApiDoc::openapi().to_json()?;
        write("openapi.json", openapi_json.as_bytes())?;
        return Ok(());
    }

    if let Some(config_file) = &config.config_file {
        let config_yaml = read(config_file).map_err(|e| {
            AnyError::msg(format!("error reading config file '{config_file}': {e}"))
        })?;
        let config_yaml = String::from_utf8_lossy(&config_yaml);
        config = serde_yaml::from_str(&config_yaml).map_err(|e| {
            AnyError::msg(format!("error parsing config file '{config_file}': {e}"))
        })?;
    }

    let config = config.canonicalize()?;

    run(config)
}

struct ServerState {
    // Serialize DB access with a lock, so we don't need to deal with
    // transaction conflicts.  The server must avoid holding this lock
    // for a long time to avoid blocking concurrent requests.
    db: Arc<Mutex<ProjectDB>>,
    // Dropping this handle kills the compiler task.
    _compiler: Option<Compiler>,
    runner: Runner,
    _config: ManagerConfig,
    pub jwk_cache: Arc<Mutex<JwkCache>>,
}

impl ServerState {
    async fn new(
        config: ManagerConfig,
        db: Arc<Mutex<ProjectDB>>,
        compiler: Option<Compiler>,
    ) -> AnyResult<Self> {
        let runner = Runner::Local(LocalRunner::new(db.clone(), &config)?);

        Ok(Self {
            db,
            _compiler: compiler,
            runner,
            _config: config,
            jwk_cache: Arc::new(Mutex::new(JwkCache::new())),
        })
    }
}

fn run(config: ManagerConfig) -> AnyResult<()> {
    // Check that the port is available before turning into a daemon, so we can fail
    // early if the port is taken.
    let listener = TcpListener::bind((config.bind_address.clone(), config.port)).map_err(|e| {
        AnyError::msg(format!(
            "failed to bind port '{}:{}': {e}",
            &config.bind_address, config.port
        ))
    })?;

    #[cfg(unix)]
    if config.unix_daemon {
        let logfile = std::fs::File::create(config.logfile.as_ref().unwrap()).map_err(|e| {
            AnyError::msg(format!(
                "failed to create log file '{}': {e}",
                &config.logfile.as_ref().unwrap()
            ))
        })?;

        let logfile_clone = logfile.try_clone().unwrap();

        let daemonize = Daemonize::new()
            .pid_file(config.manager_pid_file_path())
            .working_directory(&config.working_directory)
            .stdout(logfile_clone)
            .stderr(logfile);

        daemonize.start().map_err(|e| {
            AnyError::msg(format!(
                "failed to detach server process from terminal: '{e}'",
            ))
        })?;
    }

    let dev_mode = config.dev_mode;
    let use_auth = config.use_auth;
    rt::System::new().block_on(async {
        let db = ProjectDB::connect(&config).await?;
        let db = Arc::new(Mutex::new(db));
        let compiler = Compiler::new(&config, db.clone()).await?;

        // Since we don't trust any file system state after restart,
        // reset all programs to `ProgramStatus::None`, which will force
        // us to recompile programs before running them.
        db.lock().await.reset_program_status().await?;
        let openapi = ApiDoc::openapi();

        let state = WebData::new(ServerState::new(config, db, Some(compiler)).await?);

        if use_auth {
            let server = HttpServer::new(move || {
                let closure = |req, bearer_auth| {
                    auth::auth_validator(auth::aws_auth_config(), req, bearer_auth)
                };
                let auth_middleware = HttpAuthentication::with_fn(closure);
                let app = App::new()
                    .app_data(state.clone())
                    .wrap(Logger::default())
                    .wrap(Condition::new(dev_mode, actix_cors::Cors::permissive()))
                    .wrap(auth_middleware);

                build_app(app, openapi.clone())
            });
            server.listen(listener)?.run().await?;
        } else {
            let server = HttpServer::new(move || {
                let app = App::new()
                    .app_data(state.clone())
                    .wrap(Logger::default())
                    .wrap(Condition::new(dev_mode, actix_cors::Cors::permissive()));
                build_app(app, openapi.clone())
            });
            server.listen(listener)?.run().await?;
        }
        Ok(())
    })
}

// `static_files` magic.
include!(concat!(env!("OUT_DIR"), "/generated.rs"));

fn build_app<T>(app: App<T>, openapi: OpenApiDoc) -> App<T>
where
    T: ServiceFactory<ServiceRequest, Config = (), Error = ActixError, InitError = ()>,
{
    // Creates a dictionary of static files indexed by file name.
    let generated = generate();

    app.service(list_programs)
        .service(program_code)
        .service(program_status)
        .service(new_program)
        .service(update_program)
        .service(compile_program)
        .service(delete_program)
        .service(new_pipeline)
        .service(update_pipeline)
        .service(list_pipelines)
        .service(pipeline_stats)
        .service(pipeline_status)
        .service(pipeline_action)
        .service(pipeline_delete)
        .service(list_connectors)
        .service(new_connector)
        .service(update_connector)
        .service(connector_status)
        .service(delete_connector)
        .service(http_input)
        .service(SwaggerUi::new("/swagger-ui/{_:.*}").url("/api-doc/openapi.json", openapi))
        .service(ResourceFiles::new("/", generated))
}

/// Pipeline manager error response.
#[derive(Serialize, ToSchema)]
pub(crate) struct ErrorResponse {
    #[schema(example = "Unknown program id 42.")]
    message: String,
}

impl ErrorResponse {
    pub(crate) fn new(message: &str) -> Self {
        Self {
            message: message.to_string(),
        }
    }
}

fn http_resp_from_error(error: &AnyError) -> HttpResponse {
    debug!("Received {:?}", error);
    if let Some(db_error) = error.downcast_ref::<DBError>() {
        let message = db_error.to_string();
        match db_error {
            DBError::UnknownProgram(_) => HttpResponse::NotFound(),
            DBError::DuplicateName => HttpResponse::Conflict(),
            DBError::OutdatedProgramVersion(_) => HttpResponse::Conflict(),
            DBError::UnknownPipeline(_) => HttpResponse::NotFound(),
            DBError::UnknownConnector(_) => HttpResponse::NotFound(),
            // This error should never bubble up till here
            DBError::DuplicateKey => HttpResponse::InternalServerError(),
            DBError::InvalidKey => HttpResponse::Unauthorized(),
            DBError::UnknownName(_) => HttpResponse::NotFound(),
            // should in practice not happen, e.g., would mean a Uuid conflict:
            DBError::UniqueKeyViolation(_) => HttpResponse::InternalServerError(),
        }
        .json(ErrorResponse::new(&message))
    } else if let Some(runner_error) = error.downcast_ref::<RunnerError>() {
        let message = runner_error.to_string();
        match runner_error {
            RunnerError::PipelineShutdown(_) => HttpResponse::Conflict(),
        }
        .json(ErrorResponse::new(&message))
    } else {
        warn!("Unexpected error in http_resp_from_error: {}", error);
        warn!("Backtrace: {:#?}", error.backtrace());
        HttpResponse::InternalServerError().json(ErrorResponse::new(&error.to_string()))
    }
}

fn parse_program_id_param(req: &HttpRequest) -> Result<ProgramId, HttpResponse> {
    match req.match_info().get("program_id") {
        None => Err(HttpResponse::BadRequest().body("missing program id argument")),
        Some(program_id) => {
            match program_id.parse::<Uuid>() {
                Err(e) => Err(HttpResponse::BadRequest()
                    .body(format!("invalid program id '{program_id}': {e}"))),
                Ok(program_id) => Ok(ProgramId(program_id)),
            }
        }
    }
}

fn parse_pipeline_id_param(req: &HttpRequest) -> Result<PipelineId, HttpResponse> {
    match req.match_info().get("pipeline_id") {
        None => Err(HttpResponse::BadRequest().body("missing pipeline id argument")),
        Some(pipeline_id) => match pipeline_id.parse::<Uuid>() {
            Err(e) => Err(HttpResponse::BadRequest()
                .body(format!("invalid pipeline id '{pipeline_id}': {e}"))),
            Ok(pipeline_id) => Ok(PipelineId(pipeline_id)),
        },
    }
}

fn parse_pipeline_action(req: &HttpRequest) -> Result<&str, HttpResponse> {
    match req.match_info().get("action") {
        None => Err(HttpResponse::BadRequest().body("missing action id argument")),
        Some(action) => Ok(action),
    }
}

fn parse_connector_name_param(req: &HttpRequest) -> Result<String, HttpResponse> {
    match req.match_info().get("connector_name") {
        None => Err(HttpResponse::BadRequest().body("missing connector_name argument")),
        Some(connector_name) => match connector_name.parse::<String>() {
            Err(e) => Err(HttpResponse::BadRequest()
                .body(format!("invalid connector_name '{connector_name}': {e}"))),
            Ok(connector_name) => Ok(connector_name),
        },
    }
}

/// Enumerate the program database.
#[utoipa::path(
    responses(
        (status = OK, description = "List of programs retrieved successfully", body = [ProgramDescr]),
    ),
    tag = "Program"
)]
#[get("/v0/programs")]
async fn list_programs(state: WebData<ServerState>) -> impl Responder {
    state
        .db
        .lock()
        .await
        .list_programs()
        .await
        .map(|programs| {
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .json(programs)
        })
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// Response to a program code request.
#[derive(Serialize, ToSchema)]
struct ProgramCodeResponse {
    /// Current program meta-data.
    program: ProgramDescr,
    /// Program code.
    code: String,
}

/// Returns the latest SQL source code of the program along with its meta-data.
#[utoipa::path(
    responses(
        (status = OK, description = "Program data and code retrieved successfully.", body = ProgramCodeResponse),
        (status = BAD_REQUEST
            , description = "Missing or invalid `program_id` parameter."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Missing 'program_id' parameter."))),
        (status = NOT_FOUND
            , description = "Specified `program_id` does not exist in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Unknown program id '42'"))),
    ),
    params(
        ("program_id" = Uuid, Path, description = "Unique program identifier")
    ),
    tag = "Program"
)]
#[get("/v0/program/{program_id}/code")]
async fn program_code(state: WebData<ServerState>, req: HttpRequest) -> impl Responder {
    let program_id = match parse_program_id_param(&req) {
        Err(e) => {
            return e;
        }
        Ok(program_id) => program_id,
    };

    state
        .db
        .lock()
        .await
        .program_code(program_id)
        .await
        .map(|(program, code)| {
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .json(&ProgramCodeResponse { program, code })
        })
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// Returns program descriptor, including current program version and
/// compilation status.
#[utoipa::path(
    responses(
        (status = OK, description = "Program status retrieved successfully.", body = ProgramDescr),
        (status = BAD_REQUEST
            , description = "Missing or invalid `program_id` parameter."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Missing 'program_id' parameter."))),
        (status = NOT_FOUND
            , description = "Specified `program_id` does not exist in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Unknown program id '42'"))),
    ),
    params(
        ("id" = Option<Uuid>, Query, description = "Unique connector identifier"),
        ("name" = Option<String>, Query, description = "Unique connector name")
    ),
    tag = "Program"
)]
#[get("/v0/program")]
async fn program_status(
    state: WebData<ServerState>,
    req: web::Query<IdOrNameQuery>,
) -> impl Responder {
    let resp = if let Some(id) = req.id {
        state.db.lock().await.get_program_by_id(ProgramId(id)).await
    } else if let Some(name) = req.name.clone() {
        state.db.lock().await.get_program_by_name(&name).await
    } else {
        return HttpResponse::BadRequest().json(ErrorResponse::new("Set either `id` or `name`"));
    };

    resp.map(|descr| {
        HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&descr)
    })
    .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// Request to create a new DBSP program.
#[derive(Debug, Deserialize, ToSchema)]
struct NewProgramRequest {
    /// Program name.
    #[schema(example = "Example program")]
    name: String,
    /// Overwrite existing program with the same name, if any.
    #[serde(default)]
    overwrite_existing: bool,
    /// Program description.
    #[schema(example = "Example description")]
    description: String,
    /// SQL code of the program.
    #[schema(example = "CREATE TABLE Example(name varchar);")]
    code: String,
}

/// Response to a new program request.
#[derive(Serialize, ToSchema)]
struct NewProgramResponse {
    /// Id of the newly created program.
    #[schema(example = 42)]
    program_id: ProgramId,
    /// Initial program version (this field is always set to 1).
    #[schema(example = 1)]
    version: Version,
}

/// Create a new program.
///
/// If the `overwrite_existing` flag is set in the request and a program with
/// the same name already exists, all pipelines associated with that program and
/// the program itself will be deleted.
#[utoipa::path(
    request_body = NewProgramRequest,
    responses(
        (status = CREATED, description = "Program created successfully", body = NewProgramResponse),
        (status = CONFLICT
            , description = "A program with this name already exists in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Duplicate program name 'p'."))),
    ),
    tag = "Program"
)]
#[post("/v0/programs")]
async fn new_program(
    state: WebData<ServerState>,
    request: web::Json<NewProgramRequest>,
) -> impl Responder {
    do_new_program(state, request)
        .await
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

async fn do_new_program(
    state: WebData<ServerState>,
    request: web::Json<NewProgramRequest>,
) -> AnyResult<HttpResponse> {
    if request.overwrite_existing {
        let descr = {
            let db = state.db.lock().await;
            let descr = db.lookup_program(&request.name).await?;
            drop(db);
            descr
        };
        if let Some(program_descr) = descr {
            do_delete_program(state.clone(), program_descr.program_id).await?;
        }
    }

    state
        .db
        .lock()
        .await
        .new_program(
            Uuid::now_v7(),
            &request.name,
            &request.description,
            &request.code,
        )
        .await
        .map(|(program_id, version)| {
            HttpResponse::Created()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .json(&NewProgramResponse {
                    program_id,
                    version,
                })
        })
}

/// Update program request.
#[derive(Deserialize, ToSchema)]
struct UpdateProgramRequest {
    /// Id of the program.
    program_id: ProgramId,
    /// New name for the program.
    name: String,
    /// New description for the program.
    #[serde(default)]
    description: String,
    /// New SQL code for the program or `None` to keep existing program
    /// code unmodified.
    code: Option<String>,
}

/// Response to a program update request.
#[derive(Serialize, ToSchema)]
struct UpdateProgramResponse {
    /// New program version.  Equals the previous version if program code
    /// doesn't change or previous version +1 if it does.
    version: Version,
}

/// Change program code and/or name.
///
/// If program code changes, any ongoing compilation gets cancelled,
/// program status is reset to `None`, and program version
/// is incremented by 1.  Changing program name only doesn't affect its
/// version or the compilation process.
#[utoipa::path(
    request_body = UpdateProgramRequest,
    responses(
        (status = OK, description = "Program updated successfully.", body = UpdateProgramResponse),
        (status = NOT_FOUND
            , description = "Specified `program_id` does not exist in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Unknown program id '42'"))),
        (status = CONFLICT
            , description = "A program with this name already exists in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Duplicate program name 'p'."))),
    ),
    tag = "Program"
)]
#[patch("/v0/programs")]
async fn update_program(
    state: WebData<ServerState>,
    request: web::Json<UpdateProgramRequest>,
) -> impl Responder {
    state
        .db
        .lock()
        .await
        .update_program(
            request.program_id,
            &request.name,
            &request.description,
            &request.code,
        )
        .await
        .map(|version| {
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .json(&UpdateProgramResponse { version })
        })
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// Request to queue a program for compilation.
#[derive(Deserialize, ToSchema)]
struct CompileProgramRequest {
    /// Program id.
    program_id: ProgramId,
    /// Latest program version known to the client.
    version: Version,
}

/// Queue program for compilation.
///
/// The client should poll the `/program_status` endpoint
/// for compilation results.
#[utoipa::path(
    request_body = CompileProgramRequest,
    responses(
        (status = ACCEPTED, description = "Compilation request submitted."),
        (status = NOT_FOUND
            , description = "Specified `program_id` does not exist in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Unknown program id '42'"))),
        (status = CONFLICT
            , description = "Program version specified in the request doesn't match the latest program version in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Outdated program version '{version}'"))),
    ),
    tag = "Program"
)]
#[post("/v0/programs/compile")]
async fn compile_program(
    state: WebData<ServerState>,
    request: web::Json<CompileProgramRequest>,
) -> impl Responder {
    state
        .db
        .lock()
        .await
        .set_program_pending(request.program_id, request.version)
        .await
        .map(|_| HttpResponse::Accepted().finish())
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// Request to cancel ongoing program compilation.
#[derive(Deserialize, ToSchema)]
struct CancelProgramRequest {
    /// Program id.
    program_id: ProgramId,
    /// Latest program version known to the client.
    version: Version,
}

/// Cancel outstanding compilation request.
///
/// The client should poll the `/program_status` endpoint
/// to determine when the cancelation request completes.
#[utoipa::path(
    request_body = CancelProgramRequest,
    responses(
        (status = ACCEPTED, description = "Cancelation request submitted."),
        (status = NOT_FOUND
            , description = "Specified `program_id` does not exist in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Unknown program id '42'"))),
        (status = CONFLICT
            , description = "Program version specified in the request doesn't match the latest program version in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Outdated program version '{3}'"))),
    ),
    tag = "Program"
)]
#[delete("/v0/programs/compile")]
async fn cancel_program(
    state: WebData<ServerState>,
    request: web::Json<CancelProgramRequest>,
) -> impl Responder {
    state
        .db
        .lock()
        .await
        .cancel_program(request.program_id, request.version)
        .await
        .map(|_| HttpResponse::Accepted().finish())
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// Delete a program.
///
/// Deletes all pipelines and configs associated with the program.
#[utoipa::path(
    responses(
        (status = OK, description = "Program successfully deleted."),
        (status = NOT_FOUND
            , description = "Specified `program_id` does not exist in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Unknown program id '42'"))),
    ),
    params(
        ("program_id" = Uuid, Path, description = "Unique program identifier")
    ),
    tag = "Program"
)]
#[delete("/v0/programs/{program_id}")]
async fn delete_program(state: WebData<ServerState>, req: HttpRequest) -> impl Responder {
    let program_id = match parse_program_id_param(&req) {
        Err(e) => {
            return e;
        }
        Ok(program_id) => program_id,
    };

    do_delete_program(state, program_id)
        .await
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

async fn do_delete_program(
    state: WebData<ServerState>,
    program_id: ProgramId,
) -> AnyResult<HttpResponse> {
    let db = state.db.lock().await;
    db.delete_program(program_id)
        .await
        .map(|_| HttpResponse::Ok().finish())
}

/// Request to create a new program configuration.
#[derive(Deserialize, ToSchema)]
struct NewPipelineRequest {
    /// Config name.
    name: String,
    /// Config description.
    description: String,
    /// Program to create config for.
    program_id: Option<ProgramId>,
    /// YAML code for the config.
    config: String,
    /// Attached connectors.
    connectors: Option<Vec<AttachedConnector>>,
}

/// Response to a config creation request.
#[derive(Serialize, ToSchema)]
struct NewPipelineResponse {
    /// Id of the newly created config.
    pipeline_id: PipelineId,
    /// Initial config version (this field is always set to 1).
    version: Version,
}

/// Create a new program configuration.
#[utoipa::path(
    request_body = NewPipelineRequest,
    responses(
        (status = OK, description = "Configuration successfully created.", body = NewPipelineResponse),
        (status = NOT_FOUND
            , description = "Specified `program_id` does not exist in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Unknown program id '42'"))),
    ),
    tag = "Pipeline"
)]
#[post("/v0/pipelines")]
async fn new_pipeline(
    state: WebData<ServerState>,
    request: web::Json<NewPipelineRequest>,
) -> impl Responder {
    state
        .db
        .lock()
        .await
        .new_pipeline(
            Uuid::now_v7(),
            request.program_id,
            &request.name,
            &request.description,
            &request.config,
            &request.connectors,
        )
        .await
        .map(|(pipeline_id, version)| {
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .json(&NewPipelineResponse {
                    pipeline_id,
                    version,
                })
        })
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// Request to update an existing program configuration.
#[derive(Deserialize, ToSchema)]
struct UpdatePipelineRequest {
    /// Config id.
    pipeline_id: PipelineId,
    /// New config name.
    name: String,
    /// New config description.
    description: String,
    /// New program to create config for. If absent, program will be set to
    /// NULL.
    program_id: Option<ProgramId>,
    /// New config YAML. If absent, existing YAML will be kept unmodified.
    config: Option<String>,
    /// Attached connectors.
    ///
    /// - If absent, existing connectors will be kept unmodified.
    ///
    /// - If present all existing connectors will be replaced with the new
    /// specified list.
    connectors: Option<Vec<AttachedConnector>>,
}

/// Response to a config update request.
#[derive(Serialize, ToSchema)]
struct UpdatePipelineResponse {
    /// New config version. Equals the previous version +1.
    version: Version,
}

/// Update existing program configuration.
///
/// Updates program config name, description and code and, optionally, config
/// and connectors. On success, increments config version by 1.
#[utoipa::path(
    request_body = UpdatePipelineRequest,
    responses(
        (status = OK, description = "Configuration successfully updated.", body = UpdatePipelineResponse),
        (status = NOT_FOUND
            , description = "Specified `pipeline_id` does not exist in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Unknown config id '5'"))),
        (status = NOT_FOUND
            , description = "A connector ID in `connectors` does not exist in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Unknown connector id '5'"))),
    ),
    tag = "Pipeline"
)]
#[patch("/v0/pipelines")]
async fn update_pipeline(
    state: WebData<ServerState>,
    request: web::Json<UpdatePipelineRequest>,
) -> impl Responder {
    state
        .db
        .lock()
        .await
        .update_pipeline(
            request.pipeline_id,
            request.program_id,
            &request.name,
            &request.description,
            &request.config,
            &request.connectors,
        )
        .await
        .map(|version| {
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .json(&UpdatePipelineResponse { version })
        })
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// List pipelines.
#[utoipa::path(
    responses(
        (status = OK, description = "Pipeline list retrieved successfully.", body = [PipelineDescr])
    ),
    tag = "Pipeline"
)]
#[get("/v0/pipelines")]
async fn list_pipelines(state: WebData<ServerState>) -> impl Responder {
    state
        .db
        .lock()
        .await
        .list_pipelines()
        .await
        .map(|pipelines| {
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .json(pipelines)
        })
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// Retrieve pipeline metrics and performance counters.
#[utoipa::path(
    responses(
        // TODO: Implement `ToSchema` for `ControllerStatus`, which is the
        // actual type returned by this endpoint.
        (status = OK, description = "Pipeline metrics retrieved successfully.", body = Object),
        (status = NOT_FOUND
            , description = "Specified `pipeline_id` does not exist in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Unknown pipeline id '13'"))),
        (status = BAD_REQUEST
            , description = "Specified `pipeline_id` is not a valid uuid."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("invalid pipeline id 'abc'"))),
    ),
    params(
        ("pipeline_id" = Uuid, Path, description = "Unique pipeline identifier")
    ),
    tag = "Pipeline"
)]
#[get("/v0/pipelines/{pipeline_id}/stats")]
async fn pipeline_stats(state: WebData<ServerState>, req: HttpRequest) -> impl Responder {
    let pipeline_id = match parse_pipeline_id_param(&req) {
        Err(e) => {
            return e;
        }
        Ok(pipeline_id) => pipeline_id,
    };

    state
        .runner
        .forward_to_pipeline(pipeline_id, Method::GET, "stats")
        .await
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// Retrieve pipeline metadata.
#[utoipa::path(
    responses(
        (status = OK, description = "Pipeline descriptor retrieved successfully.", body = PipelineDescr),
        (status = NOT_FOUND
            , description = "Specified `pipeline_id` does not exist in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Unknown pipeline id '13'"))),
        (status = BAD_REQUEST
            , description = "Specified `pipeline_id` is not a valid uuid."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("invalid pipeline id 'abc'"))),
    ),
    params(
        ("id" = Option<Uuid>, Query, description = "Unique pipeline identifier"),
        ("name" = Option<String>, Query, description = "Unique pipeline name")
    ),
    tag = "Pipeline"
)]
#[get("/v0/pipeline")]
async fn pipeline_status(
    state: WebData<ServerState>,
    req: web::Query<IdOrNameQuery>,
) -> impl Responder {
    let resp: Result<db::PipelineDescr, AnyError> = if let Some(id) = req.id {
        state
            .db
            .lock()
            .await
            .get_pipeline_by_id(PipelineId(id))
            .await
    } else if let Some(name) = req.name.clone() {
        state.db.lock().await.get_pipeline_by_name(name).await
    } else {
        return HttpResponse::BadRequest().json(ErrorResponse::new("Set either `id` or `name`"));
    };

    resp.map(|descr| {
        HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&descr)
    })
    .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// Perform action on a pipeline.
///
/// - 'run': Run a new pipeline. Deploy a pipeline for the specified program and
/// configuration. This is a synchronous endpoint, which sends a response once
/// the pipeline has been initialized.
/// - 'start': Start the pipeline.
/// - 'pause': Pause the pipeline.
/// - 'shutdown': Terminate the execution of a pipeline. Sends a termination
/// request to the pipeline process. Returns immediately, without waiting for
/// the pipeline to terminate (which can take several seconds). The pipeline is
/// not deleted from the database, but its `killed` flag is set to `true`.
#[utoipa::path(
    responses(
        (status = OK
            , description = "Performed a Pipeline action."
            , content_type = "application/json"
            , body = String),
        (status = NOT_FOUND
            , description = "Specified `pipeline_id` does not exist in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Unknown pipeline id '13'"))),
        (status = BAD_REQUEST
            , description = "Specified `pipeline_id` is not a valid uuid."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("invalid pipeline id 'abc'"))),
    ),
    params(
        ("pipeline_id" = Uuid, Path, description = "Unique pipeline identifier"),
        ("action" = String, Path, description = "Pipeline action [run, start, pause, shutdown]")
    ),
    tag = "Pipeline"
)]
#[post("/v0/pipelines/{pipeline_id}/{action}")]
async fn pipeline_action(state: WebData<ServerState>, req: HttpRequest) -> impl Responder {
    let pipeline_id = match parse_pipeline_id_param(&req) {
        Err(e) => {
            return e;
        }
        Ok(pipeline_id) => pipeline_id,
    };
    let action = match parse_pipeline_action(&req) {
        Err(e) => {
            return e;
        }
        Ok(action) => action,
    };

    match action {
        "run" => state.runner.run_pipeline(pipeline_id).await,
        "start" => {
            state
                .runner
                .forward_to_pipeline(pipeline_id, Method::GET, "start")
                .await
        }
        "pause" => {
            state
                .runner
                .forward_to_pipeline(pipeline_id, Method::GET, "pause")
                .await
        }
        "shutdown" => state.runner.shutdown_pipeline(pipeline_id).await,
        _ => Ok(HttpResponse::BadRequest().body(format!("invalid action argument '{action}"))),
    }
    .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// Terminate and delete a pipeline.
///
/// Shut down the pipeline if it is still running and delete it from
/// the database.
#[utoipa::path(
    responses(
        (status = OK
            , description = "Pipeline successfully deleted."
            , content_type = "application/json"
            , body = String
            , example = json!("Pipeline successfully deleted")),
        (status = NOT_FOUND
            , description = "Specified `pipeline_id` does not exist in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Unknown pipeline id '64'"))),
        (status = INTERNAL_SERVER_ERROR
            , description = "Request failed."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Failed to shut down the pipeline; response from pipeline controller: ..."))),
    ),
    params(
        ("pipeline_id" = Uuid, Path, description = "Unique pipeline identifier")
    ),
    tag = "Pipeline"
)]
#[delete("/v0/pipelines/{pipeline_id}")]
async fn pipeline_delete(state: WebData<ServerState>, req: HttpRequest) -> impl Responder {
    let pipeline_id = match parse_pipeline_id_param(&req) {
        Err(e) => {
            return e;
        }
        Ok(pipeline_id) => pipeline_id,
    };

    let db = state.db.lock().await;

    state
        .runner
        .delete_pipeline(&db, pipeline_id)
        .await
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

fn parse_connector_id_param(req: &HttpRequest) -> Result<ConnectorId, HttpResponse> {
    match req.match_info().get("connector_id") {
        None => Err(HttpResponse::BadRequest().body("missing connector id argument")),
        Some(connector_id) => match connector_id.parse::<Uuid>() {
            Err(e) => Err(HttpResponse::BadRequest()
                .body(format!("invalid connector id '{connector_id}': {e}"))),
            Ok(connector_id) => Ok(ConnectorId(connector_id)),
        },
    }
}

/// Enumerate the connector database.
#[utoipa::path(
    responses(
        (status = OK, description = "List of connectors retrieved successfully", body = [ConnectorDescr]),
    ),
    tag = "Connector"
)]
#[get("/v0/connectors")]
async fn list_connectors(state: WebData<ServerState>) -> impl Responder {
    state
        .db
        .lock()
        .await
        .list_connectors()
        .await
        .map(|connectors| {
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .json(connectors)
        })
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// Request to create a new connector.
#[derive(Deserialize, ToSchema)]
pub(self) struct NewConnectorRequest {
    /// connector name.
    name: String,
    /// connector description.
    description: String,
    /// connector config.
    config: String,
}

/// Response to a connector creation request.
#[derive(Serialize, ToSchema)]
struct NewConnectorResponse {
    /// Unique id assigned to the new connector.
    connector_id: ConnectorId,
}

/// Create a new connector configuration.
#[utoipa::path(
    request_body = NewConnectorRequest,
    responses(
        (status = OK, description = "connector successfully created.", body = NewConnectorResponse),
    ),
    tag = "Connector"
)]
#[post("/v0/connectors")]
async fn new_connector(
    state: WebData<ServerState>,
    request: web::Json<NewConnectorRequest>,
) -> impl Responder {
    state
        .db
        .lock()
        .await
        .new_connector(
            Uuid::now_v7(),
            &request.name,
            &request.description,
            &request.config,
        )
        .await
        .map(|connector_id| {
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .json(&NewConnectorResponse { connector_id })
        })
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// Request to update an existing data-connector.
#[derive(Deserialize, ToSchema)]
struct UpdateConnectorRequest {
    /// connector id.
    connector_id: ConnectorId,
    /// New connector name.
    name: String,
    /// New connector description.
    description: String,
    /// New config YAML. If absent, existing YAML will be kept unmodified.
    config: Option<String>,
}

/// Response to a config update request.
#[derive(Serialize, ToSchema)]
struct UpdateConnectorResponse {}

/// Update existing connector.
///
/// Updates config name and, optionally, code.
/// On success, increments config version by 1.
#[utoipa::path(
    request_body = UpdateConnectorRequest,
    responses(
        (status = OK, description = "connector successfully updated.", body = UpdateConnectorResponse),
        (status = NOT_FOUND
            , description = "Specified `connector_id` does not exist in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Unknown connector id '5'"))),
    ),
    tag = "Connector"
)]
#[patch("/v0/connectors")]
async fn update_connector(
    state: WebData<ServerState>,
    request: web::Json<UpdateConnectorRequest>,
) -> impl Responder {
    state
        .db
        .lock()
        .await
        .update_connector(
            request.connector_id,
            &request.name,
            &request.description,
            &request.config,
        )
        .await
        .map(|_r| {
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .json(&UpdateConnectorResponse {})
        })
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// Delete existing connector.
#[utoipa::path(
    responses(
        (status = OK, description = "connector successfully deleted."),
        (status = NOT_FOUND
            , description = "Specified `connector_id` does not exist in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Unknown connector id '5'"))),
    ),
    params(
        ("connector_id" = Uuid, Path, description = "Unique connector identifier")
    ),
    tag = "Connector"
)]
#[delete("/v0/connectors/{connector_id}")]
async fn delete_connector(state: WebData<ServerState>, req: HttpRequest) -> impl Responder {
    let connector_id = match parse_connector_id_param(&req) {
        Err(e) => {
            return e;
        }
        Ok(connector_id) => connector_id,
    };

    state
        .db
        .lock()
        .await
        .delete_connector(connector_id)
        .await
        .map(|_| HttpResponse::Ok().finish())
        .unwrap_or_else(|e| http_resp_from_error(&e))
}

#[derive(Debug, Deserialize)]
pub struct IdOrNameQuery {
    id: Option<Uuid>,
    name: Option<String>,
}

/// Returns connector descriptor.
#[utoipa::path(
    responses(
        (status = OK, description = "connector status retrieved successfully.", body = ConnectorDescr),
        (status = BAD_REQUEST
            , description = "Missing or invalid `connector_id` parameter."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Missing 'connector_id' parameter."))),
        (status = NOT_FOUND
            , description = "Specified `connector_id` does not exist in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Unknown connector id '42'"))),
    ),
    params(
        ("id" = Option<Uuid>, Query, description = "Unique connector identifier"),
        ("name" = Option<String>, Query, description = "Unique connector name")
    ),
    tag = "Connector"
)]
#[get("/v0/connector")]
async fn connector_status(
    state: WebData<ServerState>,
    req: web::Query<IdOrNameQuery>,
) -> impl Responder {
    let resp: Result<db::ConnectorDescr, AnyError> = if let Some(id) = req.id {
        state
            .db
            .lock()
            .await
            .get_connector_by_id(ConnectorId(id))
            .await
    } else if let Some(name) = req.name.clone() {
        state.db.lock().await.get_connector_by_name(name).await
    } else {
        return HttpResponse::BadRequest().json(ErrorResponse::new("Set either `id` or `name`"));
    };

    resp.map(|descr| {
        HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&descr)
    })
    .unwrap_or_else(|e| http_resp_from_error(&e))
}

/// Connect to an HTTP input/output websocket
#[utoipa::path(
    responses(
        (status = OK
            , description = "Pipeline successfully connected to."
            , content_type = "application/json"
            , body = String
            , example = json!("Pipeline successfully connected to")),
        (status = NOT_FOUND
            , description = "Specified `pipeline_id` does not exist in the database."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Unknown pipeline id '64'"))),
        (status = NOT_FOUND
            , description = "Specified `connector_name` does not exist for the pipeline."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Unknown stream name 'MyTable'"))),
        (status = INTERNAL_SERVER_ERROR
            , description = "Request failed."
            , body = ErrorResponse
            , example = json!(ErrorResponse::new("Failed to shut down the pipeline; response from pipeline controller: ..."))),
    ),
    params(
        ("pipeline_id" = Uuid, Path, description = "Unique pipeline identifier"),
        ("connector_name" = String, Path, description = "Connector name")
    ),
    tag = "Pipeline"
)]
#[get("/v0/pipelines/{pipeline_id}/connector/{connector_name}")]
async fn http_input(
    state: WebData<ServerState>,
    req: HttpRequest,
    body: web::Payload,
) -> impl Responder {
    debug!("Received {:?}", req);

    let pipeline_id = match parse_pipeline_id_param(&req) {
        Err(e) => {
            return e;
        }
        Ok(pipeline_id) => pipeline_id,
    };
    debug!("Pipeline_id {:?}", pipeline_id);

    let connector_name = match parse_connector_name_param(&req) {
        Err(e) => return e,
        Ok(connector_name) => connector_name,
    };
    debug!("Connector name {:?}", connector_name);

    state
        .runner
        .forward_to_pipeline_as_stream(pipeline_id, connector_name.as_str(), req, body)
        .await
        .unwrap_or_else(|e| http_resp_from_error(&e))
}
