//! Feldera Pipeline Manager provides an HTTP API to catalog, compile, and
//! execute SQL programs.
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

mod auth_api;
mod connector;
mod examples;
mod http_io;
mod pipeline;
mod program;
mod service;

use crate::auth::JwkCache;
use crate::probe::Probe;
use actix_web::dev::Service;
use actix_web::Scope;
use actix_web::{
    get,
    middleware::{Condition, Logger},
    web::Data as WebData,
    web::{self},
    App, HttpRequest, HttpResponse, HttpServer,
};
use actix_web_httpauth::middleware::HttpAuthentication;
use actix_web_static_files::ResourceFiles;
use anyhow::{Error as AnyError, Result as AnyResult};
use log::info;
use pipeline_types::error::ErrorResponse;
use std::{env, net::TcpListener, sync::Arc};
use tokio::sync::Mutex;
use utoipa::{openapi::Server, Modify, OpenApi};
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;

pub(crate) use crate::compiler::ProgramStatus;
pub(crate) use crate::config::ApiServerConfig;
use crate::db::{
    AttachedConnectorId, ConnectorId, PipelineId, ProgramId, ProjectDB, ServiceId, Version,
};
pub use crate::error::ManagerError;
use crate::runner::RunnerApi;

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
        title = "Feldera API",
        description = r"
With Feldera, users create data pipelines out of SQL programs and data connectors. A SQL program comprises tables and views. Connectors feed data to input tables in a program or receive outputs computed by views.

This API allows users to create and manage data pipelines, and the programs
and connectors that comprise these pipelines.

# API concepts

* *Program*.  A SQL program with a unique name and a unique ID
  attached to it. A program contains tables and views. A program
  needs to be compiled before it can be executed in a pipeline.

* *Connector*. A data connector that can be used to feed input data to
SQL tables or consume outputs from SQL views. Every connector
has a unique name and identifier. We currently support Kafka and Redpanda.
We also support directly ingesting and consuming data via HTTP;
see the `pipelines/{pipeline_id}/ingress` and `pipelines/{pipeline_id}/egress`
endpoints.

* *Service*. A service with a unique name and ID.
  It represents a service (such as MySQL, Kafka, etc.) that a connector can refer to in
  its config. A connector can refer to zero, one or multiple services.
  Services are declared separately to reduce duplication and to make it
  easier to create connectors. A service has its own configuration, which
  generally includes hostname, port, access credentials, and any service parameters.

* *Pipeline*.  A pipeline is a running instance of a program and
some attached connectors. A client can create multiple pipelines that make use of
the same program and connectors. Every pipeline has a unique name and identifier.
Deploying a pipeline instantiates the pipeline with the then latest version of
the referenced program and connectors. This allows the API to accumulate edits
to programs and connectors before use in a pipeline.

# Concurrency

All programs have an associated *version*. This is done to prevent
race conditions due to multiple users accessing the same
program concurrently.  An example is user 1 modifying the program,
while user 2 is starting a pipeline for the same program. It would be confusing
if the pipeline could end up running the old or the new version.

A version is a monotonically increasing number, associated with each
program and pipeline. Every request to compile the program or start a
pipeline must include the program id and version number. If the version number
isn't equal to the current version in the database, this means that the
last version of the program observed by the client is outdated, so the
request is rejected."
    ),
    paths(
        program::get_programs,
        program::get_program,
        program::new_program,
        program::update_program,
        program::compile_program,
        program::delete_program,
        pipeline::new_pipeline,
        pipeline::update_pipeline,
        pipeline::list_pipelines,
        pipeline::pipeline_stats,
        pipeline::get_pipeline,
        pipeline::get_pipeline_config,
        pipeline::pipeline_validate,
        pipeline::pipeline_action,
        pipeline::pipeline_deployed,
        pipeline::pipeline_delete,
        connector::list_connectors,
        connector::get_connector,
        connector::new_connector,
        connector::update_connector,
        connector::delete_connector,
        service::list_services,
        service::get_service,
        service::new_service,
        service::update_service,
        service::delete_service,
        http_io::http_input,
        http_io::http_output,
        auth_api::get_authentication_config,
    ),
    components(schemas(
        crate::auth::AuthProvider,
        crate::auth::ProviderAwsCognito,
        crate::auth::ProviderGoogleIdentity,
        crate::compiler::SqlCompilerMessage,
        crate::db::AttachedConnector,
        crate::db::ProgramDescr,
        crate::db::ProgramSchema,
        crate::db::Relation,
        crate::db::Field,
        crate::db::ColumnType,
        crate::db::ConnectorDescr,
        crate::db::ServiceDescr,
        crate::db::Pipeline,
        crate::db::PipelineRuntimeState,
        crate::db::PipelineDescr,
        crate::db::PipelineRevision,
        crate::db::Revision,
        crate::db::PipelineStatus,
        pipeline_types::query::NeighborhoodQuery,
        pipeline_types::query::OutputQuery,
        pipeline_types::config::PipelineConfig,
        pipeline_types::config::InputEndpointConfig,
        pipeline_types::config::OutputEndpointConfig,
        pipeline_types::config::TransportConfig,
        pipeline_types::config::FormatConfig,
        pipeline_types::config::RuntimeConfig,
        pipeline_types::config::ConnectorConfig,
        pipeline_types::config::TransportConfig,
        pipeline_types::config::FormatConfig,
        pipeline_types::config::ResourceConfig,
        pipeline_types::config::ServiceConfig,
        pipeline_types::config::MysqlConfig,
        pipeline_types::config::KafkaConfig,
        pipeline_types::transport::file::FileInputConfig,
        pipeline_types::transport::file::FileOutputConfig,
        pipeline_types::transport::url::UrlInputConfig,
        pipeline_types::transport::kafka::KafkaInputConfig,
        pipeline_types::transport::kafka::KafkaInputFtConfig,
        pipeline_types::transport::kafka::KafkaOutputConfig,
        pipeline_types::transport::kafka::KafkaOutputFtConfig,
        pipeline_types::transport::kafka::KafkaLogLevel,
        pipeline_types::transport::http::Chunk,
        pipeline_types::transport::http::EgressMode,
        pipeline_types::format::csv::CsvEncoderConfig,
        pipeline_types::format::csv::CsvParserConfig,
        pipeline_types::format::json::JsonEncoderConfig,
        pipeline_types::format::json::JsonParserConfig,
        pipeline_types::format::json::JsonFlavor,
        pipeline_types::format::json::JsonUpdateFormat,
        pipeline_types::error::ErrorResponse,
        TenantId,
        ProgramId,
        PipelineId,
        ConnectorId,
        AttachedConnectorId,
        ServiceId,
        Version,
        ProgramStatus,
        ErrorResponse,
        program::ProgramCodeResponse,
        program::NewProgramRequest,
        program::NewProgramResponse,
        program::UpdateProgramRequest,
        program::UpdateProgramResponse,
        program::CompileProgramRequest,
        pipeline::NewPipelineRequest,
        pipeline::NewPipelineResponse,
        pipeline::UpdatePipelineRequest,
        pipeline::UpdatePipelineResponse,
        connector::NewConnectorRequest,
        connector::NewConnectorResponse,
        connector::UpdateConnectorRequest,
        connector::UpdateConnectorResponse,
        service::NewServiceRequest,
        service::NewServiceResponse,
        service::UpdateServiceRequest,
        service::UpdateServiceResponse,
    ),),
    tags(
        (name = "Manager", description = "Configure system behavior"),
        (name = "Programs", description = "Manage programs"),
        (name = "Pipelines", description = "Manage pipelines"),
        (name = "Connectors", description = "Manage data connectors"),
        (name = "Services", description = "Manage services"),
    ),
)]
pub struct ApiDoc;

// `static_files` magic.
include!(concat!(env!("OUT_DIR"), "/generated.rs"));

// The scope for all unauthenticated API endpoints
fn public_scope() -> Scope {
    let openapi = ApiDoc::openapi();
    // Creates a dictionary of static files indexed by file name.
    let generated = generate();

    // Leave this as an empty prefix to load the UI by default. When constructing an
    // app, always attach other scopes without empty prefixes before this one,
    // or route resolution does not work correctly.
    web::scope("")
        .service(auth_api::get_authentication_config)
        .service(SwaggerUi::new("/swagger-ui/{_:.*}").url("/api-doc/openapi.json", openapi))
        .service(healthz)
        .service(ResourceFiles::new("/", generated))
}

// The scope for all authenticated API endpoints
fn api_scope() -> Scope {
    // Make APIs available under the /v0/ prefix
    web::scope("/v0")
        .service(program::get_programs)
        .service(program::get_program)
        .service(program::new_program)
        .service(program::update_program)
        .service(program::compile_program)
        .service(program::delete_program)
        .service(pipeline::new_pipeline)
        .service(pipeline::update_pipeline)
        .service(pipeline::list_pipelines)
        .service(pipeline::pipeline_stats)
        .service(pipeline::get_pipeline)
        .service(pipeline::get_pipeline_config)
        .service(pipeline::pipeline_action)
        .service(pipeline::pipeline_validate)
        .service(pipeline::pipeline_deployed)
        .service(pipeline::pipeline_delete)
        .service(connector::list_connectors)
        .service(connector::get_connector)
        .service(connector::new_connector)
        .service(connector::update_connector)
        .service(connector::delete_connector)
        .service(service::list_services)
        .service(service::get_service)
        .service(service::new_service)
        .service(service::update_service)
        .service(service::delete_service)
        .service(http_io::http_input)
        .service(http_io::http_output)
}

pub(crate) fn parse_uuid_param(
    req: &HttpRequest,
    param_name: &'static str,
) -> Result<Uuid, ManagerError> {
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

// The below types and methods are used for running the api-server

pub(crate) struct ServerState {
    // The server must avoid holding this lock for a long time to avoid blocking concurrent
    // requests.
    pub db: Arc<Mutex<ProjectDB>>,
    runner: RunnerApi,
    _config: ApiServerConfig,
    pub jwk_cache: Arc<Mutex<JwkCache>>,
    probe: Arc<Mutex<Probe>>,
}

impl ServerState {
    pub async fn new(config: ApiServerConfig, db: Arc<Mutex<ProjectDB>>) -> AnyResult<Self> {
        let runner = RunnerApi::new(db.clone());
        let db_copy = db.clone();
        Ok(Self {
            db,
            runner,
            _config: config,
            jwk_cache: Arc::new(Mutex::new(JwkCache::new())),
            probe: Probe::new(db_copy).await,
        })
    }
}

fn create_listener(api_config: &ApiServerConfig) -> AnyResult<TcpListener> {
    // Check that the port is available before turning into a daemon, so we can fail
    // early if the port is taken.
    let listener =
        TcpListener::bind((api_config.bind_address.clone(), api_config.port)).map_err(|e| {
            AnyError::msg(format!(
                "failed to bind port '{}:{}': {e}",
                &api_config.bind_address, api_config.port
            ))
        })?;
    Ok(listener)
}

pub async fn run(db: Arc<Mutex<ProjectDB>>, api_config: ApiServerConfig) -> AnyResult<()> {
    let listener = create_listener(&api_config)?;
    let state = WebData::new(ServerState::new(api_config.clone(), db).await?);
    let auth_configuration = match api_config.auth_provider {
        crate::config::AuthProviderType::None => None,
        crate::config::AuthProviderType::AwsCognito => Some(crate::auth::aws_auth_config()),
        crate::config::AuthProviderType::GoogleIdentity => Some(crate::auth::google_auth_config()),
    };
    let server = match auth_configuration {
        Some(auth_configuration) => {
            let server = HttpServer::new(move || {
                let auth_middleware = HttpAuthentication::with_fn(crate::auth::auth_validator);

                App::new()
                    .app_data(state.clone())
                    .app_data(auth_configuration.clone())
                    .wrap(Logger::default())
                    .wrap(Condition::new(
                        api_config.dev_mode,
                        actix_cors::Cors::permissive(),
                    ))
                    .service(api_scope().wrap(auth_middleware))
                    .service(public_scope())
            });
            server.listen(listener)?.run()
        }
        None => {
            let server = HttpServer::new(move || {
                App::new()
                    .app_data(state.clone())
                    .wrap(Logger::default())
                    .wrap(Condition::new(
                        api_config.dev_mode,
                        actix_cors::Cors::permissive(),
                    ))
                    .service(api_scope().wrap_fn(|req, srv| {
                        let req = crate::auth::tag_with_default_tenant_id(req);
                        srv.call(req)
                    }))
                    .service(public_scope())
            });
            server.listen(listener)?.run()
        }
    };

    let addr = env::var("BANNER_ADDR").unwrap_or(api_config.bind_address);
    let url = format!("http://{}:{}", addr, api_config.port);
    info!(
        r"
                    Welcome to

███████ ███████ ██      ██████  ███████ ██████   █████
██      ██      ██      ██   ██ ██      ██   ██ ██   ██
█████   █████   ██      ██   ██ █████   █████   ███████
██      ██      ██      ██   ██ ██      ██  ██  ██   ██
██      ███████ ███████ ██████  ███████ ██   ██ ██   ██

Web UI URL: {}
API server URL: {}
Documentation: https://www.feldera.com/docs/
Version: {}
        ",
        url,
        url,
        env!("CARGO_PKG_VERSION")
    );
    server.await?;
    Ok(())
}

/// This is an internal endpoint and as such is not exposed via OpenAPI
#[get("/healthz")]
async fn healthz(state: WebData<ServerState>) -> Result<HttpResponse, ManagerError> {
    let probe = state.probe.lock().await;
    probe.status_as_http_response()
}
