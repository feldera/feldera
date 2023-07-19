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

use crate::auth::JwkCache;
use crate::config::{CompilerConfig, DatabaseConfig};
use actix_web::dev::Service;
use actix_web::Scope;
use actix_web::{
    delete, get,
    http::{
        header::{CacheControl, CacheDirective},
        Method,
    },
    middleware::{Condition, Logger},
    patch, post, rt,
    web::Data as WebData,
    web::{self, ReqData},
    App, HttpRequest, HttpResponse, HttpServer,
};
use actix_web_httpauth::middleware::HttpAuthentication;
use actix_web_static_files::ResourceFiles;
use anyhow::{Error as AnyError, Result as AnyResult};
#[cfg(unix)]
use daemonize::Daemonize;
use dbsp_adapters::{ControllerError, ErrorResponse};
use log::debug;
use serde::{Deserialize, Serialize};
use std::{env, net::TcpListener, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use utoipa::{openapi::Server, IntoParams, Modify, OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;
use uuid::{uuid, Uuid};

pub(crate) use crate::compiler::{Compiler, ProgramStatus};
pub(crate) use crate::config::ManagerConfig;
use crate::db::{
    storage::Storage, AttachedConnector, AttachedConnectorId, ConnectorId, DBError, PipelineId,
    PipelineRevision, PipelineStatus, ProgramDescr, ProgramId, ProjectDB, Version,
};
pub use crate::error::ManagerError;
use crate::runner::{Runner, RunnerError};

use crate::auth::TenantId;

struct ServerAddon;

// We use this to add a server variable to the OpenAPI spec
// rendered by the swagger-ui
// https://docs.rs/utoipa/1.0.1/utoipa/trait.Modify.html
//
// Note, even though this percolates to the OpenAPI spec,
// the openapi-python-generator ignores it.
// See: https://github.com/openapi-generators/openapi-python-client/issues/112
impl Modify for ServerAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        openapi.servers = Some(vec![Server::new("/v0")])
    }
}

#[derive(OpenApi)]
#[openapi(
    modifiers(&ServerAddon),
    info(
        title = "DBSP API",
        description = r"
With Feldera, users create data pipelines out of SQL programs and data connectors. An SQL program comprises tables and views. Connectors feed data to input tables in a program or receive outputs computed by views.

This API allows users to create and manage data pipelines, and the programs
and connectors that comprise these pipelines.

# API concepts

* *Program*.  An SQL program with a unique name and a unique ID
  attached to it. A program contains tables and views. A program
  needs to be compiled before it can be executed in a pipeline.
  Compiling a program also reports errors in the program if
  they exist.

* *Connector*.  A data connector that can be used to feed input data to
a program (tables) or consume outputs from a program (views). Every connector
has a unique name and identifier. We currently support Kafka, Redpanda
and HTTP connectors. HTTP connectors are implicitly created for every pipeline
for convenience.

* *Pipeline*.  A pipeline is a running instance of a program and
some attached connectors. A client can create multiple pipelines that make use of
the same program and connectors. Every pipeline has a unique name and identifier.
Pipelines need to be explicitly *committed* before use. Committing a pipeline
instantiates the pipeline with the then latest version of the referenced program and
connectors. This allows the API to accumulate edits to programs and connectors
before use.

# Concurrency

All programs and pipelines have an associated *version*. This is done to prevent
race conditions due to multiple users accessing the same
program or configuration concurrently.  An example is user 1 modifying the program,
while user 2 is starting a pipeline for the same program. It would be confusing
if the pipeline could end up running the old or the new version.

A version is a monotonically increasing version number, associated with each
program and pipeline. Every request to compile the program or start a
pipeline must include the program id and version number. If the version number
isn't equal to the current version in the database, this means that the
last version of the program observed by the client is outdated, so the
request is rejected."
    ),
    paths(
        get_programs,
        get_program,
        new_program,
        update_program,
        compile_program,
        delete_program,
        new_pipeline,
        update_pipeline,
        list_pipelines,
        pipeline_stats,
        pipeline_status,
        pipeline_validate,
        pipeline_action,
        pipeline_committed,
        pipeline_delete,
        list_connectors,
        new_connector,
        update_connector,
        connector_status,
        delete_connector,
        http_input,
        http_output,
    ),
    components(schemas(
        crate::compiler::SqlCompilerMessage,
        crate::db::AttachedConnector,
        crate::db::ProgramDescr,
        crate::db::ProgramSchema,
        crate::db::Relation,
        crate::db::Field,
        crate::db::ConnectorDescr,
        crate::db::Pipeline,
        crate::db::PipelineRuntimeState,
        crate::db::PipelineDescr,
        crate::db::PipelineRevision,
        crate::db::Revision,
        crate::db::PipelineStatus,
        dbsp_adapters::EgressMode,
        dbsp_adapters::PipelineConfig,
        dbsp_adapters::InputEndpointConfig,
        dbsp_adapters::NeighborhoodQuery,
        dbsp_adapters::OutputEndpointConfig,
        dbsp_adapters::OutputQuery,
        dbsp_adapters::TransportConfig,
        dbsp_adapters::FormatConfig,
        dbsp_adapters::transport::FileInputConfig,
        dbsp_adapters::transport::FileOutputConfig,
        dbsp_adapters::transport::KafkaInputConfig,
        dbsp_adapters::transport::KafkaOutputConfig,
        dbsp_adapters::transport::KafkaLogLevel,
        dbsp_adapters::transport::http::Chunk,
        dbsp_adapters::format::CsvEncoderConfig,
        dbsp_adapters::format::CsvParserConfig,
        TenantId,
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

pub(crate) struct ServerState {
    // Serialize DB access with a lock, so we don't need to deal with
    // transaction conflicts.  The server must avoid holding this lock
    // for a long time to avoid blocking concurrent requests.
    pub db: Arc<Mutex<ProjectDB>>,
    _compiler: Option<Compiler>,
    runner: Runner,
    _config: ManagerConfig,
    pub jwk_cache: Arc<Mutex<JwkCache>>,
}

impl ServerState {
    pub async fn new(
        config: ManagerConfig,
        compiler_config: CompilerConfig,
        db: Arc<Mutex<ProjectDB>>,
    ) -> AnyResult<Self> {
        let runner = Runner::local(db.clone(), &compiler_config);
        let compiler = Compiler::new(&compiler_config, db.clone()).await?;

        Ok(Self {
            db,
            _compiler: Some(compiler),
            runner,
            _config: config,
            jwk_cache: Arc::new(Mutex::new(JwkCache::new())),
        })
    }
}

pub fn run(
    database_config: DatabaseConfig,
    manager_config: ManagerConfig,
    compiler_config: CompilerConfig,
) -> AnyResult<()> {
    // Check that the port is available before turning into a daemon, so we can fail
    // early if the port is taken.
    let listener = TcpListener::bind((manager_config.bind_address.clone(), manager_config.port))
        .map_err(|e| {
            AnyError::msg(format!(
                "failed to bind port '{}:{}': {e}",
                &manager_config.bind_address, manager_config.port
            ))
        })?;

    #[cfg(unix)]
    if manager_config.unix_daemon {
        let logfile =
            std::fs::File::create(manager_config.logfile.as_ref().unwrap()).map_err(|e| {
                AnyError::msg(format!(
                    "failed to create log file '{}': {e}",
                    &manager_config.logfile.as_ref().unwrap()
                ))
            })?;

        let logfile_clone = logfile.try_clone().unwrap();

        let daemonize = Daemonize::new()
            .pid_file(manager_config.manager_pid_file_path())
            .working_directory(&manager_config.manager_working_directory)
            .stdout(logfile_clone)
            .stderr(logfile);

        daemonize.start().map_err(|e| {
            AnyError::msg(format!(
                "failed to detach server process from terminal: '{e}'",
            ))
        })?;
    }

    let dev_mode = manager_config.dev_mode;
    let use_auth = manager_config.use_auth;
    rt::System::new().block_on(async {
        let db = ProjectDB::connect(
            &database_config,
            #[cfg(feature = "pg-embed")]
            Some(&manager_config),
        )
        .await?;
        let db = Arc::new(Mutex::new(db));

        // Since we don't trust any file system state after restart,
        // reset all programs to `ProgramStatus::None`, which will force
        // us to recompile programs before running them.
        db.lock().await.reset_program_status().await?;

        let state = WebData::new(ServerState::new(manager_config, compiler_config, db).await?);

        if use_auth {
            let server = HttpServer::new(move || {
                let auth_middleware = HttpAuthentication::with_fn(crate::auth::auth_validator);
                let auth_configuration = crate::auth::aws_auth_config();

                App::new()
                    .app_data(state.clone())
                    .app_data(auth_configuration)
                    .wrap(Logger::default())
                    .wrap(Condition::new(dev_mode, actix_cors::Cors::permissive()))
                    .service(api_scope().wrap(auth_middleware))
                    .service(static_website_scope())
            });
            server.listen(listener)?.run().await?;
        } else {
            let server = HttpServer::new(move || {
                App::new()
                    .app_data(state.clone())
                    .wrap(Logger::default())
                    .wrap(Condition::new(dev_mode, actix_cors::Cors::permissive()))
                    .service(api_scope().wrap_fn(|req, srv| {
                        let req = crate::auth::tag_with_default_tenant_id(req);
                        srv.call(req)
                    }))
                    .service(static_website_scope())
            });
            server.listen(listener)?.run().await?;
        }
        Ok(())
    })
}

// `static_files` magic.
include!(concat!(env!("OUT_DIR"), "/generated.rs"));

fn static_website_scope() -> Scope {
    let openapi = ApiDoc::openapi();
    // Creates a dictionary of static files indexed by file name.
    let generated = generate();

    // Leave this is an empty prefix to load the UI by default. When constructing an app, always
    // attach other scopes without empty prefixes before this one, or route resolution does
    // not work correctly.
    web::scope("")
        .service(SwaggerUi::new("/swagger-ui/{_:.*}").url("/api-doc/openapi.json", openapi))
        .service(ResourceFiles::new("/", generated))
}

fn api_scope() -> Scope {
    // Make APIs available under the /v0/ prefix
    web::scope("/v0")
        .service(get_programs)
        .service(get_program)
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
        .service(pipeline_validate)
        .service(pipeline_committed)
        .service(pipeline_delete)
        .service(list_connectors)
        .service(new_connector)
        .service(update_connector)
        .service(connector_status)
        .service(delete_connector)
        .service(http_input)
        .service(http_output)
}

// Example errors for use in OpenApi docs.

fn example_pipeline_toml() -> String {
    let input_connector = crate::db::ConnectorDescr {
        connector_id: ConnectorId(uuid!("01890c99-376f-743e-ac30-87b6c0ce74ef")),
        name: "Input".into(),
        description: "My Input Connector".into(),
        config: "format: csv\n".into(),
    };
    let input = crate::db::AttachedConnector {
        name: "Input-To-Table".into(),
        is_input: true,
        connector_id: input_connector.connector_id,
        config: "my_input_table".into(),
    };
    let output_connector = crate::db::ConnectorDescr {
        connector_id: ConnectorId(uuid!("01890c99-3734-7052-9e97-55c0679a5adb")),
        name: "Output ".into(),
        description: "My Output Connector".into(),
        config: "format: csv\n".into(),
    };
    let output = crate::db::AttachedConnector {
        name: "Output-To-View".into(),
        is_input: false,
        connector_id: output_connector.connector_id,
        config: "my_output_view".into(),
    };
    let pipeline = crate::db::PipelineDescr {
        pipeline_id: PipelineId(uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8")),
        program_id: Some(ProgramId(uuid!("2e79afe1-ff4d-44d3-af5f-9397de7746c0"))),
        name: "My Pipeline".into(),
        description: "My Description".into(),
        config: "workers: 8\n".into(),
        attached_connectors: vec![input, output],
        version: Version(1),
    };

    let connectors = vec![input_connector, output_connector];
    PipelineRevision::generate_toml_config(&pipeline, &connectors).unwrap()
}

fn example_unknown_program() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::UnknownProgram {
        program_id: ProgramId(uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8")),
    })
}

fn example_program_in_use_by_pipeline() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::ProgramInUseByPipeline {
        program_id: ProgramId(uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8")),
    })
}

fn example_duplicate_name() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::DuplicateName)
}

fn example_outdated_program_version() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::OutdatedProgramVersion {
        expected_version: Version(5),
    })
}

fn example_unknown_pipeline() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::UnknownPipeline {
        pipeline_id: PipelineId(uuid!("2e79afe1-ff4d-44d3-af5f-9397de7746c0")),
    })
}

fn example_unknown_connector() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::UnknownConnector {
        connector_id: ConnectorId(uuid!("d764b9e2-19f2-4572-ba20-8b42641b07c4")),
    })
}

fn example_unknown_name() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::UnknownName {
        name: "unknown_name".to_string(),
    })
}

fn example_unknown_input_table(table: &str) -> ErrorResponse {
    ErrorResponse::from_error_nolog(&ControllerError::unknown_input_stream(
        "input_endpoint1",
        table,
    ))
}

fn example_unknown_output_table(table: &str) -> ErrorResponse {
    ErrorResponse::from_error_nolog(&ControllerError::unknown_output_stream(
        "output_endpoint1",
        table,
    ))
}

fn example_unknown_input_format() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&ControllerError::unknown_input_format(
        "input_endpoint1",
        "xml",
    ))
}

fn example_parse_error() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&ControllerError::parse_error(
        "api-ingress-my_table-d24e60a3-9058-4751-aa6b-b88f4ddfd7bd",
        anyhow::Error::msg("missing field 'column_name'"),
    ))
}

fn example_unknown_output_format() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&ControllerError::unknown_output_format(
        "output_endpoint1",
        "xml",
    ))
}

fn example_pipeline_shutdown() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&RunnerError::PipelineShutdown {
        pipeline_id: PipelineId(uuid!("2e79afe1-ff4d-44d3-af5f-9397de7746c0")),
    })
}

fn example_program_not_set() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::ProgramNotSet)
}

fn example_program_not_compiled() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::ProgramNotCompiled)
}

fn example_program_has_errors() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::ProgramFailedToCompile)
}

fn example_pipline_invalid_input_ac() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::TablesNotInSchema {
        missing: vec![("ac_name".to_string(), "my_table".to_string())],
    })
}

fn example_pipline_invalid_output_ac() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::ViewsNotInSchema {
        missing: vec![("ac_name".to_string(), "my_view".to_string())],
    })
}

fn example_pipeline_timeout() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&RunnerError::PipelineInitializationTimeout {
        pipeline_id: PipelineId(uuid!("2e79afe1-ff4d-44d3-af5f-9397de7746c0")),
        timeout: Duration::from_millis(10_000),
    })
}

fn example_invalid_uuid_param() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&ManagerError::InvalidUuidParam{value: "not_a_uuid".to_string(), error: "invalid character: expected an optional prefix of `urn:uuid:` followed by [0-9a-fA-F-], found `n` at 1".to_string()})
}

fn example_pipeline_not_specified() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&ManagerError::PipelineNotSpecified)
}

fn example_connector_not_specified() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&ManagerError::ConnectorNotSpecified)
}

fn example_invalid_pipeline_action() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&ManagerError::InvalidPipelineAction {
        action: "my_action".to_string(),
    })
}

fn example_illegal_pipeline_action() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&RunnerError::IllegalPipelineStateTransition {
            pipeline_id: PipelineId(uuid!("2e79afe1-ff4d-44d3-af5f-9397de7746c0")),
            error: "Cannot restart the pipeline while it is shutting down. Wait for the shutdown to complete before starting a new instance of the pipeline.".to_string(),
            current_status: PipelineStatus::ShuttingDown,
            desired_status: PipelineStatus::Shutdown,
            requested_status: Some(PipelineStatus::Running),
    })
}

fn example_cannot_delete_when_running() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&RunnerError::IllegalPipelineStateTransition {
            pipeline_id: PipelineId(uuid!("2e79afe1-ff4d-44d3-af5f-9397de7746c0")),
            error: "Cannot delete a running pipeline. Shutdown the pipeline first by invoking the '/shutdown' endpoint.".to_string(),
            current_status: PipelineStatus::Running,
            desired_status: PipelineStatus::Running,
            requested_status: None,
    })
}

fn parse_uuid_param(req: &HttpRequest, param_name: &'static str) -> Result<Uuid, ManagerError> {
    match req.match_info().get(param_name) {
        None => Err(ManagerError::MissingUrlEncodedParam { param: param_name }),
        Some(id) => match id.parse::<Uuid>() {
            Err(e) => Err(ManagerError::InvalidUuidParam {
                value: id.to_string(),
                error: e.to_string(),
            }),
            Ok(uuid) => Ok(uuid),
        },
    }
}

fn parse_pipeline_action(req: &HttpRequest) -> Result<&str, ManagerError> {
    match req.match_info().get("action") {
        None => Err(ManagerError::MissingUrlEncodedParam { param: "action" }),
        Some(action) => Ok(action),
    }
}

/// Response to a program code request.
#[derive(Serialize, ToSchema)]
struct ProgramCodeResponse {
    /// Current program meta-data.
    program: ProgramDescr,
    /// Program code.
    code: String,
}

/// Fetch programs, optionally filtered by name or ID.
#[utoipa::path(
    responses(
        (status = OK, description = "Programs retrieved successfully.", body = [ProgramDescr]),
        (status = NOT_FOUND
            , description = "Specified program name does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_name())),
        (status = NOT_FOUND
            , description = "Specified program id does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_program())),
    ),
    params(ProgramIdOrNameQuery, WithCodeQuery),
    tag = "Program"
)]
#[get("/programs")]
async fn get_programs(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: web::Query<ProgramIdOrNameQuery>,
    with_code: web::Query<WithCodeQuery>,
) -> Result<HttpResponse, ManagerError> {
    let with_code = with_code.with_code.unwrap_or(false);
    if let Some(id) = req.id {
        let program = state
            .db
            .lock()
            .await
            .get_program_by_id(*tenant_id, ProgramId(id), with_code)
            .await?;

        Ok(HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&vec![program]))
    } else if let Some(name) = req.name.clone() {
        let program = state
            .db
            .lock()
            .await
            .get_program_by_name(*tenant_id, &name, with_code)
            .await?;
        Ok(HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&vec![program]))
    } else {
        let programs = state
            .db
            .lock()
            .await
            .list_programs(*tenant_id, with_code)
            .await?;
        Ok(HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&programs))
    }
}

/// Fetch a program by ID.
#[utoipa::path(
    responses(
        (status = OK, description = "Program retrieved successfully.", body = ProgramDescr),
        (status = NOT_FOUND
            , description = "Specified program id does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_program())),
    ),
    params(
        ("program_id" = Uuid, Path, description = "Unique program identifier"),
        WithCodeQuery
    ),
    tag = "Program"
)]
#[get("/programs/{program_id}")]
async fn get_program(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
    query: web::Query<WithCodeQuery>,
) -> Result<HttpResponse, ManagerError> {
    let program_id = ProgramId(parse_uuid_param(&req, "program_id")?);
    let with_code = query.with_code.unwrap_or(false);
    let program = state
        .db
        .lock()
        .await
        .get_program_by_id(*tenant_id, program_id, with_code)
        .await?;

    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&program))
}

/// Request to create a new DBSP program.
#[derive(Debug, Deserialize, ToSchema)]
struct NewProgramRequest {
    /// Program name.
    #[schema(example = "Example program")]
    name: String,
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
#[utoipa::path(
    request_body = NewProgramRequest,
    responses(
        (status = CREATED, description = "Program created successfully", body = NewProgramResponse),
        (status = CONFLICT
            , description = "A program with this name already exists in the database."
            , body = ErrorResponse
            , example = json!(example_duplicate_name())),
    ),
    tag = "Program"
)]
#[post("/programs")]
async fn new_program(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: web::Json<NewProgramRequest>,
) -> Result<HttpResponse, DBError> {
    do_new_program(state, tenant_id, request).await
}

async fn do_new_program(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: web::Json<NewProgramRequest>,
) -> Result<HttpResponse, DBError> {
    state
        .db
        .lock()
        .await
        .new_program(
            *tenant_id,
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

/// Change one or more of a program's code, description or name.
///
/// If a program's code changes, any ongoing compilation gets cancelled,
/// the program status is reset to `None`, and the program version
/// is incremented by 1.
///
/// Changing only the program's name or description does not affect its
/// version or the compilation process.
#[utoipa::path(
    request_body = UpdateProgramRequest,
    responses(
        (status = OK, description = "Program updated successfully.", body = UpdateProgramResponse),
        (status = NOT_FOUND
            , description = "Specified program id does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_program())),
        (status = CONFLICT
            , description = "A program with this name already exists in the database."
            , body = ErrorResponse
            , example = json!(example_duplicate_name())),
    ),
    params(
        ("program_id" = Uuid, Path, description = "Unique program identifier")
    ),
    tag = "Program"
)]
#[patch("/programs/{program_id}")]
async fn update_program(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: HttpRequest,
    body: web::Json<UpdateProgramRequest>,
) -> Result<HttpResponse, ManagerError> {
    let program_id = ProgramId(parse_uuid_param(&request, "program_id")?);
    let version = state
        .db
        .lock()
        .await
        .update_program(
            *tenant_id,
            program_id,
            &body.name,
            &body.description,
            &body.code,
        )
        .await?;

    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&UpdateProgramResponse { version }))
}

/// Request to queue a program for compilation.
#[derive(Deserialize, ToSchema)]
struct CompileProgramRequest {
    /// Latest program version known to the client.
    version: Version,
}

/// Mark a program for compilation.
///
/// The client can track a program's compilation status by pollling the
/// `/program/{program_id}` or `/programs` endpoints, and
/// then checking the `status` field of the program object
#[utoipa::path(
    request_body = CompileProgramRequest,
    responses(
        (status = ACCEPTED, description = "Compilation request submitted."),
        (status = NOT_FOUND
            , description = "Specified program id does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_program())),
        (status = CONFLICT
            , description = "Program version specified in the request doesn't match the latest program version in the database."
            , body = ErrorResponse
            , example = json!(example_outdated_program_version())),
    ),
    params(
        ("program_id" = Uuid, Path, description = "Unique program identifier")
    ),
    tag = "Program"
)]
#[post("/programs/{program_id}/compile")]
async fn compile_program(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: HttpRequest,
    body: web::Json<CompileProgramRequest>,
) -> Result<HttpResponse, ManagerError> {
    let program_id = ProgramId(parse_uuid_param(&request, "program_id")?);
    state
        .db
        .lock()
        .await
        .set_program_pending(*tenant_id, program_id, body.version)
        .await?;

    Ok(HttpResponse::Accepted().finish())
}

/// Delete a program.
#[utoipa::path(
    responses(
        (status = OK, description = "Program successfully deleted."),
        (status = BAD_REQUEST
            , description = "Specified program id is referenced by a pipeline or is not a valid uuid."
            , body = ErrorResponse
            , examples (
                ("Program in use" =
                    (description = "Specified program id is referenced by a pipeline",
                      value = json!(example_program_in_use_by_pipeline()))),
                ("Invalid uuid" =
                    (description = "Specified program id is not a valid uuid.",
                     value = json!(example_invalid_uuid_param()))),
            )
        ),
        (status = NOT_FOUND
            , description = "Specified program id does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_program())),
    ),
    params(
        ("program_id" = Uuid, Path, description = "Unique program identifier")
    ),
    tag = "Program"
)]
#[delete("/programs/{program_id}")]
async fn delete_program(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let program_id = ProgramId(parse_uuid_param(&req, "program_id")?);

    Ok(do_delete_program(state, *tenant_id, program_id).await?)
}

async fn do_delete_program(
    state: WebData<ServerState>,
    tenant_id: TenantId,
    program_id: ProgramId,
) -> Result<HttpResponse, DBError> {
    let db = state.db.lock().await;
    db.delete_program(tenant_id, program_id)
        .await
        .map(|_| HttpResponse::Ok().finish())
}

/// Request to create a new program configuration.
#[derive(Debug, Deserialize, ToSchema)]
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
            , description = "Specified program id does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_program())),
    ),
    tag = "Pipeline"
)]
#[post("/pipelines")]
async fn new_pipeline(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: web::Json<NewPipelineRequest>,
) -> Result<HttpResponse, DBError> {
    debug!("Received new-pipeline request: {request:?}");
    let (pipeline_id, version) = state
        .db
        .lock()
        .await
        .new_pipeline(
            *tenant_id,
            Uuid::now_v7(),
            request.program_id,
            &request.name,
            &request.description,
            &request.config,
            &request.connectors,
        )
        .await?;

    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&NewPipelineResponse {
            pipeline_id,
            version,
        }))
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

/// Update existing pipeline configuration.
///
/// Updates pipeline configuration. On success, increments pipeline version by 1.
#[utoipa::path(
    request_body = UpdatePipelineRequest,
    responses(
        (status = OK, description = "Pipeline successfully updated.", body = UpdatePipelineResponse),
        (status = NOT_FOUND
            , description = "Specified pipeline id does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_pipeline())),
        (status = NOT_FOUND
            , description = "Specified connector id does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_connector())),
    ),
    tag = "Pipeline"
)]
#[patch("/pipelines")]
async fn update_pipeline(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: web::Json<UpdatePipelineRequest>,
) -> Result<HttpResponse, DBError> {
    let version = state
        .db
        .lock()
        .await
        .update_pipeline(
            *tenant_id,
            request.pipeline_id,
            request.program_id,
            &request.name,
            &request.description,
            &request.config,
            &request.connectors,
        )
        .await?;

    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&UpdatePipelineResponse { version }))
}

/// List pipelines.
#[utoipa::path(
    responses(
        (status = OK, description = "Pipeline list retrieved successfully.", body = [Pipeline])
    ),
    tag = "Pipeline"
)]
#[get("/pipelines")]
async fn list_pipelines(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
) -> Result<HttpResponse, DBError> {
    let pipelines = state.db.lock().await.list_pipelines(*tenant_id).await?;

    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(pipelines))
}

/// Return the last committed (and running, if pipeline is started)
/// configuration of the pipeline.
#[utoipa::path(
    responses(
        (status = OK, description = "Last committed configuration of the pipeline retrieved successfully (returns null if pipeline was never deployed yet).", body = Option<PipelineRevision>),
        (status = NOT_FOUND
            , description = "Specified `pipeline_id` does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_pipeline())),
    ),
    params(
        ("pipeline_id" = Uuid, Path, description = "Unique pipeline identifier")
    ),
    tag = "Pipeline"
)]
#[get("/pipelines/{pipeline_id}/committed")]
async fn pipeline_committed(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_id = PipelineId(parse_uuid_param(&req, "pipeline_id")?);

    let descr: Option<crate::db::PipelineRevision> = match state
        .db
        .lock()
        .await
        .get_last_committed_pipeline_revision(*tenant_id, pipeline_id)
        .await
    {
        Ok(revision) => Some(revision),
        Err(e) => {
            if matches!(e, DBError::NoRevisionAvailable { .. }) {
                None
            } else {
                Err(e)?
            }
        }
    };

    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&descr))
}

/// Retrieve pipeline metrics and performance counters.
#[utoipa::path(
    responses(
        // TODO: Implement `ToSchema` for `ControllerStatus`, which is the
        // actual type returned by this endpoint.
        (status = OK, description = "Pipeline metrics retrieved successfully.", body = Object),
        (status = BAD_REQUEST
            , description = "Specified pipeline id is not a valid uuid."
            , body = ErrorResponse
            , example = json!(example_invalid_uuid_param())),
        (status = NOT_FOUND
            , description = "Specified pipeline id does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_pipeline())),
    ),
    params(
        ("pipeline_id" = Uuid, Path, description = "Unique pipeline identifier")
    ),
    tag = "Pipeline"
)]
#[get("/pipelines/{pipeline_id}/stats")]
async fn pipeline_stats(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_id = PipelineId(parse_uuid_param(&req, "pipeline_id")?);

    state
        .runner
        .forward_to_pipeline(*tenant_id, pipeline_id, Method::GET, "stats")
        .await
}

/// Retrieve pipeline configuration and runtime state.
///
/// When invoked without the `?toml` flag, this endpoint
/// returns pipeline state, including static configuration and runtime status,
/// in the JSON format.  The `?toml` flag changes the behavior of this
/// endpoint to return static pipeline configuratiin in the TOML format.
// TODO: explain what this TOML is for.
#[utoipa::path(
    responses(
        (status = OK, description = "Pipeline descriptor retrieved successfully.",content(
            ("text/plain" = String, example = json!(example_pipeline_toml())),
            ("application/json" = Pipeline),
        )),
        (status = BAD_REQUEST
            , description = "Pipeline not specified. Use ?id or ?name query strings in the URL."
            , body = ErrorResponse
            , example = json!(example_pipeline_not_specified())),
        (status = NOT_FOUND
            , description = "Specified pipeline name does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_name())),
        (status = NOT_FOUND
            , description = "Specified pipeline id does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_pipeline())),
    ),
    params(
        ("id" = Option<Uuid>, Query, description = "Unique pipeline identifier"),
        ("name" = Option<String>, Query, description = "Unique pipeline name"),
        ("toml" = Option<bool>, Query, description = "Set to true to request the configuration of the pipeline as a toml file."),
    ),
    tag = "Pipeline"
)]
#[get("/pipeline")]
async fn pipeline_status(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: web::Query<PipelineIdOrNameTomlQuery>,
) -> Result<HttpResponse, ManagerError> {
    if req.toml.unwrap_or(false) {
        let toml: String = if let Some(id) = req.id {
            state
                .db
                .lock()
                .await
                .pipeline_to_toml(*tenant_id, PipelineId(id))
                .await?
        } else if let Some(name) = req.name.clone() {
            let db: tokio::sync::MutexGuard<'_, ProjectDB> = state.db.lock().await;
            let pipeline = db.get_pipeline_descr_by_name(*tenant_id, name).await?;
            db.pipeline_to_toml(*tenant_id, pipeline.pipeline_id)
                .await?
        } else {
            Err(ManagerError::PipelineNotSpecified)?
        };

        Ok(HttpResponse::Ok()
            .content_type("text/plain")
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .body(toml))
    } else {
        let pipeline: crate::db::Pipeline = if let Some(id) = req.id {
            state
                .db
                .lock()
                .await
                .get_pipeline_by_id(*tenant_id, PipelineId(id))
                .await?
        } else if let Some(name) = req.name.clone() {
            state
                .db
                .lock()
                .await
                .get_pipeline_by_name(*tenant_id, name)
                .await?
        } else {
            Err(ManagerError::PipelineNotSpecified)?
        };

        Ok(HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&pipeline))
    }
}

/// Validate the configuration of a  a pipeline.
///
/// Validate configuration, usable as a pre-cursor for deploy to
/// check if pipeline configuration is valid and can be deployed.
#[utoipa::path(
    responses(
        (status = OK
            , description = "Validate a Pipeline config."
            , content_type = "application/json"
            , body = String),
        (status = BAD_REQUEST
            , description = "Specified pipeline id is not a valid uuid."
            , body = ErrorResponse
            , example = json!(example_invalid_uuid_param())),
        (status = NOT_FOUND
            , description = "Specified pipeline id does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_pipeline())),
        (status = BAD_REQUEST
            , description = "Pipeline does not have a program set."
            , body = ErrorResponse
            , example = json!(example_program_not_set())),
        (status = SERVICE_UNAVAILABLE
            , description = "The program associated with this pipeline has not been compiled."
            , body = ErrorResponse
            , example = json!(example_program_not_compiled())),
        (status = BAD_REQUEST
            , description = "The program associated with the pipeline raised compilation error."
            , body = ErrorResponse
            , example = json!(example_program_has_errors())),
        (status = BAD_REQUEST
            , description = "The connectors in the config reference a table that doesn't exist."
            , body = ErrorResponse
            , example = json!(example_pipline_invalid_input_ac())),
        (status = BAD_REQUEST
            , description = "The connectors in the config reference a view that doesn't exist."
            , body = ErrorResponse
            , example = json!(example_pipline_invalid_output_ac())),
    ),
    params(
        ("pipeline_id" = Uuid, Path, description = "Unique pipeline identifier"),
    ),
    tag = "Pipeline"
)]
#[get("/pipelines/{pipeline_id}/validate")]
async fn pipeline_validate(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_id = PipelineId(parse_uuid_param(&req, "pipeline_id")?);

    let db = state.db.lock().await;
    Ok(db
        .pipeline_is_committable(*tenant_id, pipeline_id)
        .await
        .map(|_| HttpResponse::Ok().json("Pipeline successfully validated."))?)
}

/// Change the desired state of the pipeline.
///
/// This endpoint allows the user to control the execution of the pipeline,
/// by changing its desired state attribute (see the discussion of the desired
/// state model in the [`PipelineStatus`] documentation).
///
/// The endpoint returns immediately after validating the request and forwarding
/// it to the pipeline. The requested status change completes asynchronously.  On success,
/// the pipeline enters the requested desired state.  On error, the pipeline
/// transitions to the `Failed` state. The user
/// can monitor the current status of the pipeline by polling the `GET /pipeline`
/// endpoint.
///
/// The following values of the `action` argument are accepted by this endpoint:
///
/// - 'start': Start processing data.
/// - 'pause': Pause the pipeline.
/// - 'shutdown': Terminate the execution of the pipeline.
#[utoipa::path(
    responses(
        (status = ACCEPTED
            , description = "Request accepted."),
        (status = BAD_REQUEST
            , description = "Invalid action specified."
            , body = ErrorResponse
            , example = json!(example_invalid_pipeline_action())),
        (status = BAD_REQUEST
            , description = "Specified pipeline id is not a valid uuid."
            , body = ErrorResponse
            , example = json!(example_invalid_uuid_param())),
        (status = NOT_FOUND
            , description = "Specified pipeline id does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_pipeline())),
        (status = BAD_REQUEST
            , description = "Pipeline does not have a program set."
            , body = ErrorResponse
            , example = json!(example_program_not_set())),
        (status = SERVICE_UNAVAILABLE
            , description = "The program associated with this pipeline has not been compiled."
            , body = ErrorResponse
            , example = json!(example_program_not_compiled())),
        (status = BAD_REQUEST
            , description = "The program associated with the pipeline raised compilation error."
            , body = ErrorResponse
            , example = json!(example_program_has_errors())),
        (status = BAD_REQUEST
            , description = "The connectors in the config references a table that doesn't exist."
            , body = ErrorResponse
            , example = json!(example_pipline_invalid_input_ac())),
        (status = BAD_REQUEST
            , description = "The connectors in the config references a view that doesn't exist."
            , body = ErrorResponse
            , example = json!(example_pipline_invalid_output_ac())),
        (status = BAD_REQUEST
            , description = "Action is not applicable in the current state of the pipeline."
            , body = ErrorResponse
            , example = json!(example_illegal_pipeline_action())),
        (status = INTERNAL_SERVER_ERROR
            , description = "Timeout waiting for the pipeline to initialize."
            , body = ErrorResponse
            , example = json!(example_pipeline_timeout())),
    ),
    params(
        ("pipeline_id" = Uuid, Path, description = "Unique pipeline identifier"),
        ("action" = String, Path, description = "Pipeline action [start, pause, shutdown]")
    ),
    tag = "Pipeline"
)]
#[post("/pipelines/{pipeline_id}/{action}")]
async fn pipeline_action(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_id = PipelineId(parse_uuid_param(&req, "pipeline_id")?);
    let action = parse_pipeline_action(&req)?;

    match action {
        "start" => state.runner.start_pipeline(*tenant_id, pipeline_id).await?,
        "pause" => state.runner.pause_pipeline(*tenant_id, pipeline_id).await?,
        "shutdown" => {
            state
                .runner
                .shutdown_pipeline(*tenant_id, pipeline_id)
                .await?
        }
        _ => Err(ManagerError::InvalidPipelineAction {
            action: action.to_string(),
        })?,
    }

    Ok(HttpResponse::Accepted().finish())
}

/// Delete a pipeline.
///
/// Deletes the pipeline.  The pipeline must not be executing.
#[utoipa::path(
    responses(
        (status = OK
            , description = "Pipeline successfully deleted."),
        (status = BAD_REQUEST
            , description = "Specified pipeline id is not a valid uuid."
            , body = ErrorResponse
            , example = json!(example_invalid_uuid_param())),
        (status = NOT_FOUND
            , description = "Specified pipeline id does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_pipeline())),
        (status = BAD_REQUEST
            , description = "Pipeline cannot be deleted while executing. Shutdown the pipeine first."
            , body = ErrorResponse
            , example = json!(example_cannot_delete_when_running())),
    ),
    params(
        ("pipeline_id" = Uuid, Path, description = "Unique pipeline identifier")
    ),
    tag = "Pipeline"
)]
#[delete("/pipelines/{pipeline_id}")]
async fn pipeline_delete(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_id = PipelineId(parse_uuid_param(&req, "pipeline_id")?);

    state
        .runner
        .delete_pipeline(*tenant_id, pipeline_id)
        .await?;

    Ok(HttpResponse::Ok().finish())
}

/// Enumerate the connector database.
#[utoipa::path(
    responses(
        (status = OK, description = "List of connectors retrieved successfully", body = [ConnectorDescr]),
    ),
    tag = "Connector"
)]
#[get("/connectors")]
async fn list_connectors(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
) -> Result<HttpResponse, DBError> {
    let connectors = state.db.lock().await.list_connectors(*tenant_id).await?;

    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(connectors))
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
#[post("/connectors")]
async fn new_connector(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: web::Json<NewConnectorRequest>,
) -> Result<HttpResponse, DBError> {
    let connector_id = state
        .db
        .lock()
        .await
        .new_connector(
            *tenant_id,
            Uuid::now_v7(),
            &request.name,
            &request.description,
            &request.config,
        )
        .await?;

    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&NewConnectorResponse { connector_id }))
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
            , description = "Specified connector id does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_connector())),
    ),
    tag = "Connector"
)]
#[patch("/connectors")]
async fn update_connector(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: web::Json<UpdateConnectorRequest>,
) -> Result<HttpResponse, DBError> {
    state
        .db
        .lock()
        .await
        .update_connector(
            *tenant_id,
            request.connector_id,
            &request.name,
            &request.description,
            &request.config,
        )
        .await?;

    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&UpdateConnectorResponse {}))
}

/// Delete existing connector.
#[utoipa::path(
    responses(
        (status = OK, description = "connector successfully deleted."),
        (status = BAD_REQUEST
            , description = "Specified connector id is not a valid uuid."
            , body = ErrorResponse
            , example = json!(example_invalid_uuid_param())),
        (status = NOT_FOUND
            , description = "Specified connector id does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_connector())),
    ),
    params(
        ("connector_id" = Uuid, Path, description = "Unique connector identifier")
    ),
    tag = "Connector"
)]
#[delete("/connectors/{connector_id}")]
async fn delete_connector(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let connector_id = ConnectorId(parse_uuid_param(&req, "connector_id")?);

    state
        .db
        .lock()
        .await
        .delete_connector(*tenant_id, connector_id)
        .await?;

    Ok(HttpResponse::Ok().finish())
}

// Duplicate the same structure twice, since
// these doc comments will be automatically used
// in the OpenAPI spec.
#[derive(Debug, Deserialize, IntoParams)]
pub struct ConnectorIdOrNameQuery {
    /// Unique connector identifier.
    id: Option<Uuid>,
    /// Unique connector name.
    name: Option<String>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct ProgramIdOrNameQuery {
    /// Unique program identifier.
    id: Option<Uuid>,
    /// Unique program name.
    name: Option<String>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct WithCodeQuery {
    /// Option to include the SQL program code or not
    /// in the Program objects returned by the query.
    /// If false (default), the returned program object
    /// will not include the code.
    with_code: Option<bool>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct PipelineIdOrNameTomlQuery {
    /// Unique pipeline id.
    id: Option<Uuid>,
    /// Unique pipeline name.
    name: Option<String>,
    /// Set to true to request the configuration of the pipeline as a toml file.
    toml: Option<bool>,
}

/// Returns connector descriptor.
#[utoipa::path(
    responses(
        (status = OK, description = "connector status retrieved successfully.", body = ConnectorDescr),
        (status = BAD_REQUEST
            , description = "Connector not specified. Use ?id or ?name query strings in the URL."
            , body = ErrorResponse
            , example = json!(example_connector_not_specified())),
        (status = NOT_FOUND
            , description = "Specified connector name does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_name())),
        (status = NOT_FOUND
            , description = "Specified connector id does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_connector())),
        (status = NOT_FOUND
            , description = "Specified connector name does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_name())),
    ),
    params(
        ("id" = Option<Uuid>, Query, description = "Unique connector identifier"),
        ("name" = Option<String>, Query, description = "Unique connector name")
    ),
    tag = "Connector"
)]
#[get("/connector")]
async fn connector_status(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: web::Query<ConnectorIdOrNameQuery>,
) -> Result<HttpResponse, ManagerError> {
    let descr: crate::db::ConnectorDescr = if let Some(id) = req.id {
        state
            .db
            .lock()
            .await
            .get_connector_by_id(*tenant_id, ConnectorId(id))
            .await?
    } else if let Some(name) = req.name.clone() {
        state
            .db
            .lock()
            .await
            .get_connector_by_name(*tenant_id, name)
            .await?
    } else {
        Err(ManagerError::ConnectorNotSpecified)?
    };

    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&descr))
}

/// Push data to a SQL table.
///
/// The client sends data encoded using the format specified in the `?format=`
/// parameter as a body of the request.  The contents of the data must match
/// the SQL table schema specified in `table_name`
///
/// The pipeline ingests data as it arrives without waiting for the end of
/// the request.  Successful HTTP response indicates that all data has been
/// ingested successfully.
// TODO: implement chunked and batch modes.
#[utoipa::path(
    responses(
        (status = OK
            , description = "Data successfully delivered to the pipeline."
            , content_type = "application/json"),
        (status = BAD_REQUEST
            , description = "Specified pipeline id is not a valid uuid."
            , body = ErrorResponse
            , example = json!(example_invalid_uuid_param())),
        (status = NOT_FOUND
            , description = "Specified pipeline id does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_pipeline())),
        (status = NOT_FOUND
            , description = "Specified table does not exist."
            , body = ErrorResponse
            , example = json!(example_unknown_input_table("MyTable"))),
        (status = NOT_FOUND
            , description = "Pipeline is not currently running because it has been shutdown or not yet started."
            , body = ErrorResponse
            , example = json!(example_pipeline_shutdown())),
        (status = BAD_REQUEST
            , description = "Unknown data format specified in the '?format=' argument."
            , body = ErrorResponse
            , example = json!(example_unknown_input_format())),
        (status = UNPROCESSABLE_ENTITY
            , description = "Error parsing input data."
            , body = ErrorResponse
            , example = json!(example_parse_error())),
        (status = INTERNAL_SERVER_ERROR
            , description = "Request failed."
            , body = ErrorResponse),
    ),
    params(
        ("pipeline_id" = Uuid, Path, description = "Unique pipeline identifier."),
        ("table_name" = String, Path, description = "SQL table name."),
        ("format" = String, Query, description = "Input data format, e.g., 'csv' or 'json'."),
    ),
    tag = "Pipeline"
)]
#[post("/pipelines/{pipeline_id}/ingress/{table_name}")]
async fn http_input(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
    body: web::Payload,
) -> Result<HttpResponse, ManagerError> {
    debug!("Received {req:?}");

    let pipeline_id = PipelineId(parse_uuid_param(&req, "pipeline_id")?);
    debug!("Pipeline_id {:?}", pipeline_id);

    let table_name = match req.match_info().get("table_name") {
        None => {
            return Err(ManagerError::MissingUrlEncodedParam {
                param: "table_name",
            });
        }
        Some(table_name) => table_name,
    };
    debug!("Table name {table_name:?}");

    let endpoint = format!("ingress/{table_name}");

    state
        .runner
        .forward_to_pipeline_as_stream(*tenant_id, pipeline_id, &endpoint, req, body)
        .await
}

/// Subscribe to a stream of updates to a SQL view or table.
///
/// The pipeline responds with a continuous stream of changes to the specified
/// table or view, encoded using the format specified in the `?format=`
/// parameter. Updates are split into `Chunk`'s.
///
/// The pipeline continuous sending updates until the client closes the
/// connection or the pipeline is shut down.
#[utoipa::path(
    responses(
        (status = OK
            , description = "Connection to the endpoint successfully established. The body of the response contains a stream of data chunks."
            , content_type = "application/json"
            , body = Chunk),
        (status = BAD_REQUEST
            , description = "Specified pipeline id is not a valid uuid."
            , body = ErrorResponse
            , example = json!(example_invalid_uuid_param())),
        (status = NOT_FOUND
            , description = "Specified pipeline id does not exist in the database."
            , body = ErrorResponse
            , example = json!(example_unknown_pipeline())),
        (status = NOT_FOUND
            , description = "Specified table or view does not exist."
            , body = ErrorResponse
            , example = json!(example_unknown_output_table("MyTable"))),
        (status = GONE
            , description = "Pipeline is not currently running because it has been shutdown or not yet started."
            , body = ErrorResponse
            , example = json!(example_pipeline_shutdown())),
        (status = BAD_REQUEST
            , description = "Unknown data format specified in the '?format=' argument."
            , body = ErrorResponse
            , example = json!(example_unknown_output_format())),
        (status = INTERNAL_SERVER_ERROR
            , description = "Request failed."
            , body = ErrorResponse),
    ),
    params(
        ("pipeline_id" = Uuid, Path, description = "Unique pipeline identifier."),
        ("table_name" = String, Path, description = "SQL table or view name."),
        ("format" = String, Query, description = "Output data format, e.g., 'csv' or 'json'."),
        ("query" = Option<OutputQuery>, Query, description = "Query to execute on the table. Must be one of 'table', 'neighborhood', or 'quantiles'. The default value is 'table'"),
        ("mode" = Option<EgressMode>, Query, description = "Output mode. Must be one of 'watch' or 'snapshot'. The default value is 'watch'"),
        ("quantiles" = Option<u32>, Query, description = "For 'quantiles' queries: the number of quantiles to output. The default value is 100."),
    ),
    request_body(
        content = Option<NeighborhoodQuery>,
        description = "When the `query` parameter is set to 'neighborhood', the body of the request must contain a neighborhood specification.",
        content_type = "application/json",
    ),
    tag = "Pipeline"
)]
#[get("/pipelines/{pipeline_id}/egress/{table_name}")]
async fn http_output(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
    body: web::Payload,
) -> Result<HttpResponse, ManagerError> {
    debug!("Received {req:?}");

    let pipeline_id = PipelineId(parse_uuid_param(&req, "pipeline_id")?);
    debug!("Pipeline_id {:?}", pipeline_id);

    let table_name = match req.match_info().get("table_name") {
        None => {
            return Err(ManagerError::MissingUrlEncodedParam {
                param: "table_name",
            });
        }
        Some(table_name) => table_name,
    };
    debug!("Table name {table_name:?}");

    let endpoint = format!("egress/{table_name}");

    state
        .runner
        .forward_to_pipeline_as_stream(*tenant_id, pipeline_id, &endpoint, req, body)
        .await
}
