use crate::api::demo::{read_demos_from_directories, Demo};
use crate::api::endpoints;
use crate::auth::JwkCache;
use crate::cluster_health::HealthStatus;
use crate::config::{ApiServerConfig, CommonConfig};
use crate::db::probe::DbProbe;
use crate::db::storage_postgres::StoragePostgres;
use crate::error::ManagerError;
use crate::license::LicenseCheck;
use crate::runner::interaction::RunnerInteraction;
use crate::unstable_features;
use actix_http::body::BoxBody;
use actix_http::StatusCode;
use actix_web::body::MessageBody;
use actix_web::dev::{Service, ServiceResponse};
use actix_web::http::Method;
use actix_web::Scope;
use actix_web::{
    get,
    web::Data as WebData,
    web::{self},
    App, HttpResponse, HttpServer,
};
use actix_web_httpauth::middleware::HttpAuthentication;
use actix_web_static_files::ResourceFiles;
use anyhow::Result as AnyResult;
use futures_util::FutureExt;
use log::{error, info, log, trace, Level};
use std::io::Write;
use std::time::Duration;
use std::{env, io, net::TcpListener, sync::Arc};
use termbg::{theme, Theme};
use tokio::sync::{Mutex, RwLock};
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};
use utoipa_swagger_ui::SwaggerUi;

#[derive(OpenApi)]
#[openapi(
    modifiers(&SecurityAddon),
    info(
        title = "Feldera API",
        description = r"
With Feldera, users create data pipelines out of SQL programs.
A SQL program comprises tables and views, and includes as well the definition of
input and output connectors for each respectively. A connector defines a data
source or data sink to feed input data into tables or receive output data
computed by the views respectively.

## Pipeline

The API is centered around the **pipeline**, which most importantly consists
out of the SQL program, but also has accompanying metadata and configuration parameters
(e.g., compilation profile, number of workers, etc.).

* A pipeline is identified and referred to by its user-provided unique name.
* The pipeline program is asynchronously compiled when the pipeline is first created or
  when its program is subsequently updated.
* Pipeline deployment is only possible once the program is successfully compiled.
* A pipeline cannot be updated while it is deployed.

## Concurrency

Each pipeline has a version, which is incremented each time its core fields are updated.
The version is monotonically increasing. There is additionally a program version which covers
only the program-related core fields, and is used by the compiler to discern when to recompile."
    ),
    paths(
        // Pipeline management endpoints
        endpoints::pipeline_management::list_pipelines,
        endpoints::pipeline_management::get_pipeline,
        endpoints::pipeline_management::post_pipeline,
        endpoints::pipeline_management::put_pipeline,
        endpoints::pipeline_management::patch_pipeline,
        endpoints::pipeline_management::delete_pipeline,
        endpoints::pipeline_management::post_pipeline_start,
        endpoints::pipeline_management::post_pipeline_pause,
        endpoints::pipeline_management::post_pipeline_stop,
        endpoints::pipeline_management::post_pipeline_clear,
        endpoints::pipeline_management::get_pipeline_logs,

        // Pipeline interaction endpoints
        endpoints::pipeline_interaction::http_input,
        endpoints::pipeline_interaction::http_output,
        endpoints::pipeline_interaction::post_pipeline_input_connector_action,
        endpoints::pipeline_interaction::get_pipeline_input_connector_status,
        endpoints::pipeline_interaction::get_pipeline_output_connector_status,
        endpoints::pipeline_interaction::get_pipeline_stats,
        endpoints::pipeline_interaction::get_pipeline_metrics,
        endpoints::pipeline_interaction::get_pipeline_circuit_profile,
        endpoints::pipeline_interaction::get_pipeline_heap_profile,
        endpoints::pipeline_interaction::support_bundle::get_pipeline_support_bundle,
        endpoints::pipeline_interaction::pipeline_adhoc_sql,
        endpoints::pipeline_interaction::checkpoint_pipeline,
        endpoints::pipeline_interaction::get_checkpoint_status,
        endpoints::pipeline_interaction::completion_token,
        endpoints::pipeline_interaction::completion_status,
        endpoints::pipeline_interaction::start_transaction,
        endpoints::pipeline_interaction::commit_transaction,

        // API keys
        endpoints::api_key::list_api_keys,
        endpoints::api_key::get_api_key,
        endpoints::api_key::post_api_key,
        endpoints::api_key::delete_api_key,

        // Configuration
        endpoints::config::get_config_authentication,
        endpoints::config::get_config_demos,
        endpoints::config::get_config,

        // Metrics
        endpoints::metrics::get_metrics,

        // Cluster Health
        endpoints::cluster_healthz::get_health
    ),
    components(schemas(
        // Authentication
        crate::auth::AuthProvider,
        crate::auth::ProviderAwsCognito,
        crate::auth::ProviderGoogleIdentity,

        // Common
        crate::db::types::version::Version,
        crate::license::DisplaySchedule,
        crate::license::LicenseInformation,
        crate::license::LicenseValidity,
        crate::api::endpoints::config::UpdateInformation,
        crate::api::endpoints::config::Configuration,
        crate::api::endpoints::config::BuildInformation,

        // Pipeline
        crate::db::types::pipeline::PipelineId,
        crate::db::types::pipeline::PipelineStatus,
        crate::db::types::pipeline::PipelineDesiredStatus,
        crate::api::endpoints::pipeline_management::PipelineInfo,
        crate::api::endpoints::pipeline_management::PipelineSelectedInfo,
        crate::api::endpoints::pipeline_management::PipelineFieldSelector,
        crate::api::endpoints::pipeline_management::GetPipelineParameters,
        crate::api::endpoints::pipeline_management::PostPutPipeline,
        crate::api::endpoints::pipeline_management::PatchPipeline,
        crate::api::endpoints::pipeline_management::PostStopPipelineParameters,

        // Storage
        crate::db::types::storage::StorageStatus,

        // Demo
        crate::api::demo::Demo,

        // Program
        crate::db::types::program::CompilationProfile,
        crate::db::types::program::SqlCompilerMessage,
        crate::db::types::program::ProgramStatus,
        crate::db::types::program::ProgramError,
        crate::db::types::program::SqlCompilationInfo,
        crate::db::types::program::RustCompilationInfo,
        crate::db::types::program::ProgramConfig,
        crate::db::types::program::ProgramInfo,
        crate::api::endpoints::pipeline_management::PartialProgramInfo,

        // API key
        crate::db::types::api_key::ApiKeyId,
        crate::db::types::api_key::ApiPermission,
        crate::db::types::api_key::ApiKeyDescr,
        crate::api::endpoints::api_key::NewApiKeyRequest,
        crate::api::endpoints::api_key::NewApiKeyResponse,

        // From the feldera-types crate
        feldera_types::config::PipelineConfig,
        feldera_types::config::StorageConfig,
        feldera_types::config::StorageCacheConfig,
        feldera_types::config::StorageOptions,
        feldera_types::config::StorageBackendConfig,
        feldera_types::config::SyncConfig,
        feldera_types::config::StartFromCheckpoint,
        feldera_types::config::FileBackendConfig,
        feldera_types::config::StorageCompression,
        feldera_types::config::RuntimeConfig,
        feldera_types::config::FtConfig,
        feldera_types::config::InputEndpointConfig,
        feldera_types::config::ConnectorConfig,
        feldera_types::config::OutputBufferConfig,
        feldera_types::config::OutputEndpointConfig,
        feldera_types::config::TransportConfig,
        feldera_types::config::FormatConfig,
        feldera_types::config::ResourceConfig,
        feldera_types::config::ObjectStorageConfig,
        feldera_types::config::FtModel,
        feldera_types::transport::adhoc::AdHocInputConfig,
        feldera_types::transport::clock::ClockConfig,
        feldera_types::transport::file::FileInputConfig,
        feldera_types::transport::file::FileOutputConfig,
        feldera_types::transport::http::HttpInputConfig,
        feldera_types::transport::url::UrlInputConfig,
        feldera_types::transport::kafka::KafkaHeader,
        feldera_types::transport::kafka::KafkaHeaderValue,
        feldera_types::transport::kafka::KafkaLogLevel,
        feldera_types::transport::kafka::KafkaInputConfig,
        feldera_types::transport::kafka::KafkaOutputConfig,
        feldera_types::transport::kafka::KafkaOutputFtConfig,
        feldera_types::transport::kafka::KafkaStartFromConfig,
        feldera_types::transport::nats::NatsInputConfig,
        feldera_types::transport::pubsub::PubSubInputConfig,
        feldera_types::transport::s3::S3InputConfig,
        feldera_types::transport::datagen::DatagenStrategy,
        feldera_types::transport::datagen::RngFieldSettings,
        feldera_types::transport::datagen::GenerationPlan,
        feldera_types::transport::datagen::DatagenInputConfig,
        feldera_types::transport::nexmark::NexmarkInputConfig,
        feldera_types::transport::nexmark::NexmarkTable,
        feldera_types::transport::nexmark::NexmarkInputOptions,
        feldera_types::transport::delta_table::DeltaTableIngestMode,
        feldera_types::transport::delta_table::DeltaTableWriteMode,
        feldera_types::transport::delta_table::DeltaTableReaderConfig,
        feldera_types::transport::delta_table::DeltaTableWriterConfig,
        feldera_types::transport::iceberg::IcebergReaderConfig,
        feldera_types::transport::iceberg::IcebergIngestMode,
        feldera_types::transport::iceberg::IcebergCatalogType,
        feldera_types::transport::iceberg::RestCatalogConfig,
        feldera_types::transport::iceberg::GlueCatalogConfig,
        feldera_types::transport::postgres::PostgresReaderConfig,
        feldera_types::transport::postgres::PostgresWriterConfig,
        feldera_types::transport::redis::RedisOutputConfig,
        feldera_types::transport::http::Chunk,
        feldera_types::transport::clock::ClockConfig,
        feldera_types::query::AdhocQueryArgs,
        feldera_types::query::AdHocResultFormat,
        feldera_types::format::json::JsonUpdateFormat,
        feldera_types::format::json::JsonLines,
        feldera_types::program_schema::ProgramSchema,
        feldera_types::program_schema::Relation,
        feldera_types::program_schema::SqlType,
        feldera_types::program_schema::Field,
        feldera_types::program_schema::ColumnType,
        feldera_types::program_schema::IntervalUnit,
        feldera_types::program_schema::SourcePosition,
        feldera_types::program_schema::PropertyValue,
        feldera_types::program_schema::SqlIdentifier,
        feldera_types::query_params::MetricsFormat,
        feldera_types::query_params::MetricsParameters,
        feldera_types::error::ErrorResponse,
        feldera_types::completion_token::CompletionTokenResponse,
        feldera_types::completion_token::CompletionStatusArgs,
        feldera_types::completion_token::CompletionStatus,
        feldera_types::completion_token::CompletionStatusResponse,
        feldera_types::checkpoint::CheckpointStatus,
        feldera_types::checkpoint::CheckpointResponse,
        feldera_types::checkpoint::CheckpointFailure,
        feldera_types::transaction::StartTransactionResponse,

        // Cluster health check
        crate::cluster_health::HealthStatus,
        crate::cluster_health::ServiceStatus,
    ),),
    tags(
        (name = "Pipeline management", description = "Create, retrieve, update, delete and deploy pipelines."),
        (name = "Pipeline interaction", description = "Interact with deployed pipelines."),
        (name = "Configuration", description = "Retrieve configuration."),
        (name = "API keys", description = "Create, retrieve and delete API keys."),
        (name = "Metrics", description = "Retrieve metrics across pipelines."),
    ),
)]
pub struct ApiDoc;

// `static_files` magic.
include!(concat!(env!("OUT_DIR"), "/generated.rs"));

// The scope for all unauthenticated API endpoints
fn public_scope() -> Scope {
    let openapi = ApiDoc::openapi();

    // Leave this as an empty prefix to load the UI by default. When constructing an
    // app, always attach other scopes without empty prefixes before this one,
    // or route resolution does not work correctly.
    web::scope("")
        .service(endpoints::config::get_config_authentication)
        .service(SwaggerUi::new("/swagger-ui/{_:.*}").url("/api-doc/openapi.json", openapi))
        .service(healthz)
        .service(ResourceFiles::new("/", generate()).resolve_not_found_to_root())
}

// The scope for all authenticated API endpoints
fn api_scope() -> Scope {
    // Make APIs available under the /v0/ prefix
    web::scope("/v0")
        // Pipeline management endpoints
        .service(endpoints::pipeline_management::list_pipelines)
        .service(endpoints::pipeline_management::get_pipeline)
        .service(endpoints::pipeline_management::post_pipeline)
        .service(endpoints::pipeline_management::put_pipeline)
        .service(endpoints::pipeline_management::patch_pipeline)
        .service(endpoints::pipeline_management::delete_pipeline)
        .service(endpoints::pipeline_management::post_pipeline_start)
        .service(endpoints::pipeline_management::post_pipeline_pause)
        .service(endpoints::pipeline_management::post_pipeline_stop)
        .service(endpoints::pipeline_management::post_pipeline_clear)
        .service(endpoints::pipeline_management::get_pipeline_logs)
        // Pipeline interaction endpoints
        .service(endpoints::pipeline_interaction::http_input)
        .service(endpoints::pipeline_interaction::http_output)
        .service(endpoints::pipeline_interaction::checkpoint_pipeline)
        .service(endpoints::pipeline_interaction::sync_checkpoint)
        .service(endpoints::pipeline_interaction::get_checkpoint_status)
        .service(endpoints::pipeline_interaction::get_checkpoint_sync_status)
        .service(endpoints::pipeline_interaction::post_pipeline_input_connector_action)
        .service(endpoints::pipeline_interaction::get_pipeline_input_connector_status)
        .service(endpoints::pipeline_interaction::get_pipeline_output_connector_status)
        .service(endpoints::pipeline_interaction::get_pipeline_stats)
        .service(endpoints::pipeline_interaction::get_pipeline_metrics)
        .service(endpoints::pipeline_interaction::get_pipeline_time_series)
        .service(endpoints::pipeline_interaction::get_pipeline_circuit_profile)
        .service(endpoints::pipeline_interaction::get_pipeline_heap_profile)
        .service(endpoints::pipeline_interaction::support_bundle::get_pipeline_support_bundle)
        .service(endpoints::pipeline_interaction::pipeline_adhoc_sql)
        .service(endpoints::pipeline_interaction::completion_token)
        .service(endpoints::pipeline_interaction::completion_status)
        .service(endpoints::pipeline_interaction::start_transaction)
        .service(endpoints::pipeline_interaction::commit_transaction)
        // API keys endpoints
        .service(endpoints::api_key::list_api_keys)
        .service(endpoints::api_key::get_api_key)
        .service(endpoints::api_key::post_api_key)
        .service(endpoints::api_key::delete_api_key)
        // Configuration endpoints
        .service(endpoints::config::get_config)
        .service(endpoints::config::get_config_demos)
        // Metrics of all pipelines belonging to this tenant
        .service(endpoints::metrics::get_metrics)
        // Cluster health check
        .service(endpoints::cluster_healthz::get_health)
}

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "JSON web token (JWT) or API key",
                SecurityScheme::Http(
                    HttpBuilder::new()
                        .scheme(HttpAuthScheme::Bearer)
                        .bearer_format("JWT")
                        .description(Some(
                            r#"Use a JWT token obtained via an OAuth2/OIDC
                               login workflow or an API key obtained via
                               the `/v0/api-keys` endpoint."#,
                        ))
                        .build(),
                ),
            )
        }
    }
}

// The below types and methods are used for running the api-server

pub(crate) struct ServerState {
    // The server must avoid holding this lock for a long time to avoid blocking concurrent
    // requests.
    pub db: Arc<Mutex<StoragePostgres>>,
    pub runner: RunnerInteraction,
    pub common_config: CommonConfig,
    pub _config: ApiServerConfig,
    pub jwk_cache: Arc<Mutex<JwkCache>>,
    probe: Arc<Mutex<DbProbe>>,
    pub demos: Vec<Demo>,
    pub license_check: Arc<RwLock<Option<LicenseCheck>>>,
    pub health_check: Arc<RwLock<Option<HealthStatus>>>,
}

impl ServerState {
    pub async fn new(
        common_config: CommonConfig,
        config: ApiServerConfig,
        db: Arc<Mutex<StoragePostgres>>,
        license_check: Arc<RwLock<Option<LicenseCheck>>>,
        health_check: Arc<RwLock<Option<HealthStatus>>>,
    ) -> AnyResult<Self> {
        let runner = RunnerInteraction::new(common_config.clone(), db.clone());
        let db_copy = db.clone();
        let demos = read_demos_from_directories(&config.demos_dir);
        Ok(Self {
            db,
            runner,
            common_config,
            _config: config,
            jwk_cache: Arc::new(Mutex::new(JwkCache::new())),
            probe: DbProbe::new(db_copy).await,
            demos,
            license_check,
            health_check,
        })
    }
}

/// Logs the responses of the HTTP(S) server.
pub fn log_response(
    res: Result<ServiceResponse<BoxBody>, actix_web::Error>,
) -> Result<ServiceResponse<BoxBody>, actix_web::Error> {
    match &res {
        Ok(response) => {
            let req = response.request();
            let level = if response.status().is_success()
                || response.status().is_informational()
                || response.status().is_redirection()
            {
                if req.method() == Method::GET && req.path() == "/healthz" {
                    Level::Trace
                } else {
                    Level::Debug
                }
            } else if response.status().is_client_error()
                || response.status() == StatusCode::SERVICE_UNAVAILABLE
            {
                Level::Info
            } else {
                Level::Error
            };
            log!(
                level,
                "Response: {} (size: {:?}) to request {} {}",
                response.status(),
                response.response().body().size(),
                req.method(),
                req.path()
            );
        }
        Err(e) => {
            error!("Service response error: {e}");
        }
    }
    res
}

pub async fn run(
    db: Arc<Mutex<StoragePostgres>>,
    common_config: CommonConfig,
    api_config: ApiServerConfig,
    license_check: Arc<RwLock<Option<LicenseCheck>>>,
    health_check: Arc<RwLock<Option<HealthStatus>>>,
) -> AnyResult<()> {
    let listener = TcpListener::bind((common_config.bind_address.clone(), common_config.api_port))
        .unwrap_or_else(|_| {
            panic!(
                "API server unable to bind listener to {}:{} -- is the port occupied?",
                common_config.bind_address, common_config.api_port
            )
        });
    let state = WebData::new(
        ServerState::new(
            common_config.clone(),
            api_config.clone(),
            db,
            license_check,
            health_check,
        )
        .await?,
    );
    let auth_configuration = match api_config.auth_provider {
        crate::config::AuthProviderType::None => None,
        crate::config::AuthProviderType::AwsCognito => Some(crate::auth::aws_auth_config()),
        crate::config::AuthProviderType::GoogleIdentity => Some(crate::auth::google_auth_config()),
    };
    let server = match auth_configuration {
        // We instantiate an awc::Client that can be used if the api-server needs to
        // make outgoing calls. This object is not meant to have more than one instance
        // per thread (otherwise, it causes high resource pressure on both CPU and fds).
        Some(auth_configuration) => {
            let common_config_cloned = common_config.clone();
            let server = HttpServer::new(move || {
                let auth_middleware = HttpAuthentication::with_fn(crate::auth::auth_validator);
                let client = WebData::new(common_config_cloned.awc_client());
                App::new()
                    .app_data(state.clone())
                    .app_data(auth_configuration.clone())
                    .app_data(client)
                    .wrap_fn(|req, srv| {
                        trace!("Request: {} {}", req.method(), req.path());
                        srv.call(req).map(log_response)
                    })
                    .wrap(api_config.cors())
                    .service(api_scope().wrap(auth_middleware))
                    .service(public_scope())
            })
            .workers(common_config.http_workers)
            .worker_max_blocking_threads(std::cmp::max(512 / common_config.http_workers, 1));
            if let Some(server_config) = common_config.https_server_config() {
                server
                    .listen_rustls_0_23(listener, server_config)
                    .expect("API HTTPS server unable to listen")
                    .run()
            } else {
                server
                    .listen(listener)
                    .expect("API HTTP server unable to listen")
                    .run()
            }
        }
        None => {
            let common_config_cloned = common_config.clone();
            let server = HttpServer::new(move || {
                let client = WebData::new(common_config_cloned.awc_client());
                App::new()
                    .app_data(state.clone())
                    .app_data(client)
                    .wrap_fn(|req, srv| {
                        trace!("Request: {} {}", req.method(), req.path());
                        srv.call(req).map(log_response)
                    })
                    .wrap(api_config.cors())
                    .service(api_scope().wrap_fn(|req, srv| {
                        let req = crate::auth::tag_with_default_tenant_id(req);
                        srv.call(req)
                    }))
                    .service(public_scope())
            })
            .workers(common_config.http_workers)
            .worker_max_blocking_threads(std::cmp::max(512 / common_config.http_workers, 1));
            if let Some(server_config) = common_config.https_server_config() {
                server
                    .listen_rustls_0_23(listener, server_config)
                    .expect("API HTTPS server unable to listen")
                    .run()
            } else {
                server
                    .listen(listener)
                    .expect("API HTTP server unable to listen")
                    .run()
            }
        }
    };
    info!(
        "API {} server: ready on port {} ({} workers)",
        if common_config.enable_https {
            "HTTPS"
        } else {
            "HTTP"
        },
        common_config.api_port,
        common_config.http_workers,
    );

    let banner = if theme(Duration::from_millis(500)).unwrap_or(Theme::Light) == Theme::Dark {
        include_str!("../../light-banner.ascii")
    } else {
        include_str!("../../dark-banner.ascii")
    };
    let addr = env::var("BANNER_ADDR").unwrap_or("127.0.0.1".to_string());
    let url = format!(
        "{}://{}:{}",
        if common_config.enable_https {
            "https"
        } else {
            "http"
        },
        addr,
        common_config.api_port
    );

    // Lock both out streams so that the banner is printed in one go
    // and not interrupted by log messages from other threads.
    let err_lock = io::stderr().lock();
    let mut out_lock = io::stdout().lock();
    let _ = out_lock.write_all(
        format!(
            r"

{banner}

Web console URL: {}
API server URL: {}
Documentation: https://docs.feldera.com/
Version: {} v{}{}
",
            url,
            url,
            if cfg!(feature = "feldera-enterprise") {
                "Enterprise"
            } else {
                "Open source"
            },
            env!("CARGO_PKG_VERSION"),
            if env!("FELDERA_PLATFORM_VERSION_SUFFIX").is_empty() {
                "".to_string()
            } else {
                format!(" ({})", env!("FELDERA_PLATFORM_VERSION_SUFFIX"))
            },
        )
        .as_bytes(),
    );
    if let Some(features) = unstable_features() {
        println!(
            "Unstable Features: {}",
            features.iter().cloned().collect::<Vec<&str>>().join(", ")
        );
    }
    drop(out_lock);
    drop(err_lock);

    server.await?;
    Ok(())
}

/// This is an internal endpoint and as such is not exposed via OpenAPI
#[get("/healthz")]
async fn healthz(state: WebData<ServerState>) -> Result<HttpResponse, ManagerError> {
    let probe = state.probe.lock().await;
    probe.status_as_http_response()
}
