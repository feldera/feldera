use crate::{
    controller::ConfigError,
    transport::http::{
        HttpInputEndpoint, HttpInputTransport, HttpOutputEndpoint, HttpOutputTransport,
    },
    Catalog, Controller, ControllerError, DetailedError, FormatConfig, InputEndpoint,
    InputEndpointConfig, OutputEndpoint, OutputEndpointConfig, OutputQuery, PipelineConfig,
};
use actix_web::{
    dev::{Server, ServiceFactory, ServiceRequest},
    get,
    middleware::Logger,
    post, rt, web,
    web::{Data as WebData, Json, Payload, Query},
    App, Error as ActixError, HttpRequest, HttpResponse, HttpServer, Responder,
};
use actix_web_static_files::ResourceFiles;
use anyhow::Error as AnyError;
use clap::Parser;
use colored::Colorize;
use dbsp::{operator::sample::MAX_QUANTILES, DBSPHandle};
use env_logger::Env;
use erased_serde::Deserializer as ErasedDeserializer;
use form_urlencoded;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use serde_urlencoded::Deserializer as UrlDeserializer;
use serde_yaml::Value as YamlValue;
use std::io::Write;
use std::{
    borrow::Cow,
    error::Error as StdError,
    fmt::{Display, Error as FmtError, Formatter},
    net::TcpListener,
    sync::{Arc, Mutex},
};
use tokio::{
    spawn,
    sync::mpsc::{channel, Receiver, Sender},
};
use utoipa::ToSchema;
use uuid::Uuid;

mod prometheus;

use self::prometheus::PrometheusMetrics;

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum ApiError {
    PrometheusError {
        error: String,
    },
    MissingUrlEncodedParam {
        param: &'static str,
    },
    ApiConnectionLimit,
    TableSnapshotNotImplemented,
    QuantileStreamingNotSupported,
    NumQuantilesOutOfRange {
        quantiles: u32,
    },
    MissingNeighborhoodSpec,
    InvalidNeighborhoodSpec {
        spec: JsonValue,
        parse_error: String,
    },
}

impl StdError for ApiError {}

impl Display for ApiError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Self::PrometheusError{error} => {
                write!(f, "Error retrieving Prometheus metrics: '{error}'.")
            }
            Self::MissingUrlEncodedParam { param } => {
                write!(f, "Missing URL-encoded parameter '{param}'.")
            }
            Self::ApiConnectionLimit => {
                f.write_str("The API connections limit has been exceded. Close some of the existing connections before opening new ones.")
            }
            Self::QuantileStreamingNotSupported => {
                f.write_str("Continuous monitoring is not supported for quantiles. Use '?mode=snapshot' to retrieve a single set of quantiles.")
            }
            Self::TableSnapshotNotImplemented => {
                f.write_str("Taking a snapshot of a table or view is not yet supported.")
            }
            Self::MissingNeighborhoodSpec => {
                f.write_str(r#"Neighborhood request must specify neighborhood in the body of the request: '{"anchor": ..., "before": 100, "after": 100}'."#)
            }
            Self::NumQuantilesOutOfRange{quantiles} => {
                write!(f, "The requested number of quantiles, {quantiles}, is beyond the allowed range 1 to {MAX_QUANTILES}.")
            }
            Self::InvalidNeighborhoodSpec{spec, parse_error} => {
                write!(f, "Unable to parse neighborhood descriptor '{spec}'. Error returned by the parser: '{parse_error}'.")
            }
        }
    }
}

impl DetailedError for ApiError {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::PrometheusError { .. } => Cow::from("PrometheusError"),
            Self::MissingUrlEncodedParam { .. } => Cow::from("MissingUrlEncodedParam"),
            Self::ApiConnectionLimit => Cow::from("ApiConnectionLimit"),
            Self::QuantileStreamingNotSupported => Cow::from("QuantileStreamingNotSupported"),
            Self::TableSnapshotNotImplemented => Cow::from("TableSnapshotNotImplemented"),
            Self::MissingNeighborhoodSpec => Cow::from("MissingNeighborhoodSpec"),
            Self::NumQuantilesOutOfRange { .. } => Cow::from("NumQuantilesOutOfRange"),
            Self::InvalidNeighborhoodSpec { .. } => Cow::from("InvalidNeighborhoodSpec"),
        }
    }
}

struct ServerState {
    metadata: String,
    controller: Mutex<Option<Controller>>,
    prometheus: PrometheusMetrics,
    /// Channel used to send a `kill` command to
    /// the self-destruct task when shutting down
    /// the server.
    terminate_sender: Option<Sender<()>>,
}

impl ServerState {
    fn new(
        controller: Controller,
        prometheus: PrometheusMetrics,
        meta: String,
        terminate_sender: Option<Sender<()>>,
    ) -> Self {
        Self {
            metadata: meta,
            controller: Mutex::new(Some(controller)),
            prometheus,
            terminate_sender,
        }
    }
}

/// Information returned by REST API endpoints on error.
#[derive(Serialize, ToSchema)]
pub struct ErrorResponse {
    /// Human-readable error message.
    #[schema(example = "Unknown input format 'xml'.")]
    message: String,
    /// Error code is a string that specifies this error type.
    #[schema(example = "UnknownInputFormat")]
    error_code: Cow<'static, str>,
    /// Detailed error metadata.
    /// The contents of this field is determined by `error_code`.
    #[schema(value_type=Object)]
    details: JsonValue,
}

impl ErrorResponse {
    pub fn from_anyerror(error: &AnyError) -> Self {
        let message = error.to_string();
        let error_code = Cow::from("UnknownError");

        error!("[HTTP error response] {error_code}: {message}");
        warn!("Backtrace: {:#?}", error.backtrace());

        Self {
            message,
            error_code,
            details: json!(null),
        }
    }

    pub fn from_error<E>(error: &E) -> Self
    where
        E: DetailedError,
    {
        let message = error.to_string();
        let error_code = error.error_code();

        error!("[HTTP error response] {error_code}: {message}");

        Self {
            message,
            error_code,
            details: serde_json::to_value(error).unwrap_or_else(|e| {
                JsonValue::String(format!("Failed to serialize error. Details: '{e}'"))
            }),
        }
    }
}

fn http_resp_from_controller_error(error: &ControllerError) -> HttpResponse {
    match error {
        ControllerError::Config {
            config_error: ConfigError::UnknownInputStream { .. },
        } => HttpResponse::NotFound(),
        ControllerError::Config {
            config_error: ConfigError::UnknownOutputStream { .. },
        } => HttpResponse::NotFound(),
        ControllerError::Config { .. } => HttpResponse::BadRequest(),
        ControllerError::ParseError { .. } => HttpResponse::BadRequest(),
        ControllerError::PipelineTerminating { .. } => HttpResponse::NotFound(),
        _ => HttpResponse::InternalServerError(),
    }
    .json(ErrorResponse::from_error(error))
}

fn http_resp_from_api_error(error: &ApiError) -> HttpResponse {
    match error {
        ApiError::PrometheusError { .. } => HttpResponse::InternalServerError(),
        ApiError::MissingUrlEncodedParam { .. } => HttpResponse::BadRequest(),
        ApiError::ApiConnectionLimit => HttpResponse::TooManyRequests(),
        ApiError::QuantileStreamingNotSupported => HttpResponse::MethodNotAllowed(),
        ApiError::TableSnapshotNotImplemented => HttpResponse::NotImplemented(),
        ApiError::MissingNeighborhoodSpec => HttpResponse::BadRequest(),
        ApiError::NumQuantilesOutOfRange { .. } => HttpResponse::RangeNotSatisfiable(),
        ApiError::InvalidNeighborhoodSpec { .. } => HttpResponse::BadRequest(),
    }
    .json(ErrorResponse::from_error(error))
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Pipeline configuration YAML file
    #[arg(short, long)]
    config_file: String,

    /// Pipeline metadata JSON file
    #[arg(short, long)]
    metadata_file: Option<String>,

    /// Run the server on this port if it is available. If the port is in
    /// use or no default port is specified, an unused TCP port is allocated
    /// automatically
    #[arg(short = 'p', long)]
    default_port: Option<u16>,
}

// This file indicates the port used by the server
pub const SERVER_PORT_FILE: &str = "port";

/// Server main function.
///
/// This function is intended to be invoked from the code generated by,
/// e.g., the SQL compiler.  It performs the following steps needed to start
/// a circuit server:
///
/// * Setup logging.
/// * Parse command line arguments.
/// * Start the server.
///
/// # Arguments
///
/// * `circuit_factory` - a function that creates a circuit and builds an
///   input/output stream
/// catalog.
pub fn server_main<F>(circuit_factory: &F) -> Result<(), ControllerError>
where
    F: Fn(usize) -> (DBSPHandle, Catalog),
{
    let args = Args::try_parse().map_err(|e| ControllerError::cli_args_error(&e))?;
    let yaml_config = std::fs::read(&args.config_file).map_err(|e| {
        ControllerError::io_error(
            format!("reading configuration file '{}'", args.config_file),
            &e,
        )
    })?;
    let yaml_config = String::from_utf8(yaml_config)
        .map_err(|e| ControllerError::config_parse_error(&format!("invalid UTF8 string ({e})")))?;
    let config: PipelineConfig = serde_yaml::from_str(yaml_config.as_str()).map_err(|e| {
        let err = format!("error parsing pipeline configuration: {e}");
        error!("{err}");
        ControllerError::config_parse_error(&e)
    })?;

    // Create env logger.
    let pipeline_name = format!("[{}]", config.name.clone().unwrap_or_default()).cyan();
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format(move |buf, record| {
            let t = chrono::Utc::now();
            let t = format!("{}", t.format("%Y-%m-%d %H:%M:%S"));
            writeln!(
                buf,
                "{t} {} {pipeline_name} {}",
                buf.default_styled_level(record.level()),
                record.args()
            )
        })
        .init();

    let meta = match args.metadata_file {
        None => String::new(),
        Some(metadata_file) => {
            let meta = std::fs::read(&metadata_file).map_err(|e| {
                ControllerError::io_error(format!("reading metadata file '{}'", metadata_file), &e)
            })?;
            String::from_utf8(meta).map_err(|e| {
                ControllerError::config_parse_error(&format!(
                    "invalid UTF8 string in the metadata file ({e})"
                ))
            })?
        }
    };
    run_server(circuit_factory, &config, meta, args.default_port).map_err(|e| {
        error!("{e}");
        e
    })
}

pub fn run_server<F>(
    circuit_factory: &F,
    config: &PipelineConfig,
    meta: String,
    default_port: Option<u16>,
) -> Result<(), ControllerError>
where
    F: Fn(usize) -> (DBSPHandle, Catalog),
{
    // NOTE: The pipeline manager monitors pipeline log for one of the following
    // messages ("Failed to create pipeline..." or "Started HTTP server...").
    // If you change these messages, make sure to make a corresponding change to
    // `runner.rs`.
    let (port, server, mut terminate_receiver) =
        create_server(circuit_factory, config, meta, default_port)?;

    rt::System::new().block_on(async {
        // Spawn a task that will shutdown the server on `/kill`.
        let server_handle = server.handle();
        spawn(async move {
            terminate_receiver.recv().await;
            server_handle.stop(true).await
        });

        info!("Started HTTP server on port {port}");
        tokio::fs::write(SERVER_PORT_FILE, format!("{}\n", port))
            .await
            .map_err(|e| ControllerError::io_error("writing server port file".to_string(), &e))?;
        server
            .await
            .map_err(|e| ControllerError::io_error("in the HTTP server".to_string(), &e))
    })?;
    Ok(())
}

pub fn create_server<F>(
    circuit_factory: &F,
    config: &PipelineConfig,
    meta: String,
    default_port: Option<u16>,
) -> Result<(u16, Server, Receiver<()>), ControllerError>
where
    F: Fn(usize) -> (DBSPHandle, Catalog),
{
    let (circuit, catalog) = circuit_factory(config.global.workers as usize);

    let controller = Controller::with_config(
        circuit,
        catalog,
        config,
        Box::new(|e| error!("{e}")) as Box<dyn Fn(ControllerError) + Send + Sync>,
    )?;

    let prometheus =
        PrometheusMetrics::new(&controller).map_err(|e| ControllerError::prometheus_error(&e))?;

    let listener = match default_port {
        Some(port) => TcpListener::bind(("127.0.0.1", port))
            .or_else(|_| TcpListener::bind(("127.0.0.1", 0)))
            .map_err(|e| ControllerError::io_error(format!("binding to TCP port {port}"), &e))?,
        None => TcpListener::bind(("127.0.0.1", 0))
            .map_err(|e| ControllerError::io_error("binding to TCP port".to_string(), &e))?,
    };

    let port = listener
        .local_addr()
        .map_err(|e| {
            ControllerError::io_error(
                "retrieving local socket address of the TCP listener".to_string(),
                &e,
            )
        })?
        .port();

    let (terminate_sender, terminate_receiver) = channel(1);
    let state = WebData::new(ServerState::new(
        controller,
        prometheus,
        meta,
        Some(terminate_sender),
    ));
    let server =
        HttpServer::new(move || build_app(App::new().wrap(Logger::default()), state.clone()))
            .workers(1)
            .listen(listener)
            .map_err(|e| {
                ControllerError::io_error("binding server to the listener".to_string(), &e)
            })?
            .run();

    Ok((port, server, terminate_receiver))
}

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

fn build_app<T>(app: App<T>, state: WebData<ServerState>) -> App<T>
where
    T: ServiceFactory<ServiceRequest, Config = (), Error = ActixError, InitError = ()>,
{
    let generated = generate();

    let index_data = match generated.get("index.html") {
        None => "<html><head><title>DBSP server</title></head></html>"
            .as_bytes()
            .to_owned(),
        Some(resource) => resource.data.to_owned(),
    };

    app.app_data(state)
        .route(
            "/",
            web::get().to(move || {
                let index_data = index_data.clone();
                async { HttpResponse::Ok().body(index_data) }
            }),
        )
        .service(ResourceFiles::new("/static", generated))
        .service(start)
        .service(pause)
        .service(shutdown)
        .service(stats)
        .service(metrics)
        .service(metadata)
        .service(dump_profile)
        .service(input_endpoint)
        .service(output_endpoint)
}

#[get("/start")]
async fn start(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.lock().unwrap() {
        Some(controller) => {
            controller.start();
            HttpResponse::Ok().json("The pipeline is running")
        }
        None => http_resp_from_controller_error(&ControllerError::pipeline_terminating()),
    }
}

#[get("/pause")]
async fn pause(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.lock().unwrap() {
        Some(controller) => {
            controller.pause();
            HttpResponse::Ok().json("Pipeline paused")
        }
        None => http_resp_from_controller_error(&ControllerError::pipeline_terminating()),
    }
}

#[get("/stats")]
async fn stats(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.lock().unwrap() {
        Some(controller) => {
            let json_string = serde_json::to_string(controller.status()).unwrap();
            HttpResponse::Ok()
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        }
        None => http_resp_from_controller_error(&ControllerError::pipeline_terminating()),
    }
}

/// This endpoint is invoked by the Prometheus server.
#[get("/metrics")]
async fn metrics(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.lock().unwrap() {
        Some(controller) => match state.prometheus.metrics(controller) {
            Ok(metrics) => HttpResponse::Ok()
                .content_type(mime::TEXT_PLAIN)
                .body(metrics),
            Err(e) => http_resp_from_api_error(&ApiError::PrometheusError {
                error: e.to_string(),
            }),
        },
        None => http_resp_from_controller_error(&ControllerError::pipeline_terminating()),
    }
}

#[get("/metadata")]
async fn metadata(state: WebData<ServerState>) -> impl Responder {
    HttpResponse::Ok()
        .content_type(mime::APPLICATION_JSON)
        .body(state.metadata.clone())
}

#[get("/dump_profile")]
async fn dump_profile(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.lock().unwrap() {
        Some(controller) => {
            controller.dump_profile();
            HttpResponse::Ok().json("Profile dump initiated")
        }
        None => http_resp_from_controller_error(&ControllerError::pipeline_terminating()),
    }
}

#[get("/shutdown")]
async fn shutdown(state: WebData<ServerState>) -> impl Responder {
    let controller = state.controller.lock().unwrap().take();
    if let Some(controller) = controller {
        match controller.stop() {
            Ok(()) => {
                if let Some(sender) = &state.terminate_sender {
                    let _ = sender.send(()).await;
                }
                if let Err(e) = tokio::fs::remove_file(SERVER_PORT_FILE).await {
                    warn!("Failed to remove server port file: {e}");
                }
                HttpResponse::Ok().json("Pipeline terminated")
            }
            Err(e) => http_resp_from_controller_error(&e),
        }
    } else {
        HttpResponse::Ok().json("Pipeline already terminated")
    }
}

#[derive(Debug, Deserialize)]
struct IngressArgs {
    // #[serde(default = "HttpInputTransport::default_mode")]
    // mode: HttpIngressMode,
    #[serde(default = "HttpInputTransport::default_format")]
    format: String,
}

#[post("/ingress/{table_name}")]
async fn input_endpoint(
    state: WebData<ServerState>,
    req: HttpRequest,
    args: Query<IngressArgs>,
    payload: Payload,
) -> impl Responder {
    debug!("{req:?}");
    let table_name = match req.match_info().get("table_name") {
        None => {
            return http_resp_from_api_error(&ApiError::MissingUrlEncodedParam {
                param: "table_name",
            })
        }
        Some(table_name) => table_name.to_string(),
    };
    // debug!("Table name {table_name:?}");

    // Generate endpoint name.
    let endpoint_name = format!("api-ingress-{table_name}-{}", Uuid::new_v4());

    // Create HTTP endpoint.
    let endpoint = HttpInputEndpoint::new(&endpoint_name);

    // Create endpoint config.
    let config = InputEndpointConfig {
        transport: HttpInputTransport::config(),
        stream: Cow::from(table_name),
        format: FormatConfig {
            name: Cow::from(args.format.clone()),
            config: YamlValue::Null,
        },
        max_buffered_records: HttpInputTransport::default_max_buffered_records(),
    };

    // Connect endpoint.
    let endpoint_id = match &*state.controller.lock().unwrap() {
        Some(controller) => {
            if controller.register_api_connection().is_err() {
                return http_resp_from_api_error(&ApiError::ApiConnectionLimit);
            }

            match controller.add_input_endpoint(
                &endpoint_name,
                config,
                &mut <dyn ErasedDeserializer>::erase(UrlDeserializer::new(form_urlencoded::parse(
                    req.query_string().as_bytes(),
                ))),
                Box::new(endpoint.clone()) as Box<dyn InputEndpoint>,
            ) {
                Ok(endpoint_id) => endpoint_id,
                Err(e) => {
                    controller.unregister_api_connection();
                    debug!("Failed to create API endpoint: '{e}'");
                    return http_resp_from_controller_error(&e);
                }
            }
        }
        None => {
            return http_resp_from_controller_error(&ControllerError::pipeline_terminating());
        }
    };

    // Call endpoint to complete request.
    let response = endpoint
        .complete_request(payload)
        .await
        .unwrap_or_else(|e| http_resp_from_controller_error(&e));
    drop(endpoint);

    // Delete endpoint on completion/error.
    if let Some(controller) = state.controller.lock().unwrap().as_ref() {
        controller.disconnect_input(&endpoint_id);
        controller.unregister_api_connection();
    }

    response
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, ToSchema)]
pub enum EgressMode {
    /// Continuously monitor the output of the query.
    ///
    /// For queries that support snapshots, e.g.,
    /// [neighborhood](`OutputQuery::Neighborhood`) queries,
    /// the endpoint outputs the initial snapshot followed
    /// by a stream of deltas.  For queries that don't support
    /// snapshots, the endpoint outputs the stream of deltas
    /// relative to the current output of the query.
    #[serde(rename = "watch")]
    Watch,
    /// Output a single snapshot of query results.
    ///
    /// Currently only supported for [quantile](`OutputQuery::Quantiles`)
    /// and [neighborhood](`OutputQuery::Neighborhood`) queries.
    #[serde(rename = "snapshot")]
    Snapshot,
}

impl Default for EgressMode {
    /// If `mode` is not specified, default to `Watch`.
    fn default() -> Self {
        Self::Watch
    }
}

/// URL-encoded arguments to the `/egress` endpoint.
#[derive(Debug, Deserialize)]
struct EgressArgs {
    /// Query to execute on the table.
    #[serde(default)]
    query: OutputQuery,

    /// Output mode.
    #[serde(default)]
    mode: EgressMode,

    /// Data format used to encode the output of the query, e.g., 'csv',
    /// 'json' etc.
    #[serde(default = "HttpOutputTransport::default_format")]
    format: String,

    /// For [`quantiles`](`OutputQuery::Quantiles`) queries:
    /// the number of quantiles to output.
    #[serde(default = "dbsp::operator::sample::default_quantiles")]
    quantiles: u32,
}

#[get("/egress/{table_name}")]
async fn output_endpoint(
    state: WebData<ServerState>,
    req: HttpRequest,
    args: Query<EgressArgs>,
    body: Option<Json<JsonValue>>,
) -> impl Responder {
    debug!("/egress request:{req:?}");

    let state = state.into_inner();

    let table_name = match req.match_info().get("table_name") {
        None => {
            return http_resp_from_api_error(&ApiError::MissingUrlEncodedParam {
                param: "table_name",
            })
        }
        Some(table_name) => table_name.to_string(),
    };

    // Check for unsupported combinations.
    match (args.mode, args.query) {
        (EgressMode::Watch, OutputQuery::Quantiles) => {
            return http_resp_from_api_error(&ApiError::QuantileStreamingNotSupported);
        }
        (EgressMode::Snapshot, OutputQuery::Table) => {
            return http_resp_from_api_error(&ApiError::TableSnapshotNotImplemented);
        }
        _ => {}
    };

    if args.query == OutputQuery::Neighborhood {
        if body.is_none() {
            return http_resp_from_api_error(&ApiError::MissingNeighborhoodSpec);
        }
    } else if args.query == OutputQuery::Quantiles
        && (args.quantiles as usize > MAX_QUANTILES || args.quantiles == 0)
    {
        return http_resp_from_api_error(&ApiError::NumQuantilesOutOfRange {
            quantiles: args.quantiles,
        });
    }

    // Generate endpoint name depending on the query and output mode.
    let endpoint_name = format!(
        "api-{}-{table_name}-{}{}",
        match args.mode {
            EgressMode::Watch => "watch",
            EgressMode::Snapshot => "snapshot",
        },
        match args.query {
            OutputQuery::Table => "",
            OutputQuery::Neighborhood => "neighborhood-",
            OutputQuery::Quantiles => "quantiles-",
        },
        Uuid::new_v4()
    );

    // debug!("Endpoint name: '{endpoint_name}'");

    // Create HTTP endpoint.
    let endpoint =
        HttpOutputEndpoint::new(&endpoint_name, &args.format, args.mode == EgressMode::Watch);

    // Create endpoint config.
    let config = OutputEndpointConfig {
        stream: Cow::from(table_name),
        query: args.query,
        transport: HttpOutputTransport::config(),
        format: FormatConfig {
            name: Cow::from(args.format.clone()),
            config: YamlValue::Null,
        },
        max_buffered_records: HttpOutputTransport::default_max_buffered_records(),
    };

    // Declare `response` in this scope, before we lock `state.controller`.  This makes
    // sure that on error the finalizer for `response` also runs in this scope, preventing
    // the deadlock caused by the finalizer trying to lock the controller.
    let response: HttpResponse;

    // Connect endpoint.
    match &*state.controller.lock().unwrap() {
        Some(controller) => {
            if controller.register_api_connection().is_err() {
                return http_resp_from_api_error(&ApiError::ApiConnectionLimit);
            }

            let endpoint_id = match controller.add_output_endpoint(
                &endpoint_name,
                &config,
                &mut <dyn ErasedDeserializer>::erase(UrlDeserializer::new(form_urlencoded::parse(
                    req.query_string().as_bytes(),
                ))),
                Box::new(endpoint.clone()) as Box<dyn OutputEndpoint>,
            ) {
                Ok(endpoint_id) => endpoint_id,
                Err(e) => {
                    controller.unregister_api_connection();
                    return http_resp_from_controller_error(&e);
                }
            };

            // We need to pass a callback to `request` to disconnect the endpoint when the request
            // completes.  Use a donwgraded reference to `state`, so this closure doesn't prevent
            // the controller from shutting down.
            let weak_state = Arc::downgrade(&state);

            // Call endpoint to create a response with a streaming body, which will be evaluated
            // after we return the response object to actix.
            response = endpoint.request(Box::new(move || {
                // Delete endpoint on completion/error.
                // We don't control the lifetime of the reponse object after
                // returning it to actix, so the only way to run cleanup code
                // when the HTTP request terminates is to piggyback on the
                // destructor.
                if let Some(state) = weak_state.upgrade() {
                    // This code will be invoked from `drop`, which means that
                    // it can run as part of a panic handler, so we need to
                    // handle a poisoned lock without causing a nested panic.
                    if let Ok(guard) = state.controller.lock() {
                        if let Some(controller) = guard.as_ref() {
                            controller.disconnect_output(&endpoint_id);
                            controller.unregister_api_connection();
                        }
                    }
                }
            }));

            // The endpoint is ready to receive data from the pipeline.
            match args.query {
                // Send reset signal to produce a complete neighborhood snapshot.
                OutputQuery::Neighborhood => {
                    let body = body.unwrap();

                    if let Err(e) = controller
                        .catalog()
                        .lock()
                        .unwrap()
                        .output_handles(&config.stream)
                        // The following `unwrap` is safe because `table_name` was previously validated
                        // by `add_output_endpoint`.
                        .unwrap()
                        .neighborhood_descr_handle
                        .set_for_all(&mut <dyn ErasedDeserializer>::erase(json!([
                            json!(true),
                            body
                        ])))
                    {
                        // Dropping `response` triggers the finalizer closure, which will
                        // disconnect this endpoint.
                        return http_resp_from_api_error(&ApiError::InvalidNeighborhoodSpec {
                            spec: body.into_inner(),
                            parse_error: e.to_string(),
                        });
                    }
                    controller.request_step();
                }
                // Write quantiles size.
                OutputQuery::Quantiles => {
                    controller
                        .catalog()
                        .lock()
                        .unwrap()
                        .output_handles(&config.stream)
                        .unwrap()
                        .num_quantiles_handle
                        .set_for_all(args.quantiles as usize);
                    controller.request_step();
                }
                OutputQuery::Table => {}
            }
        }
        None => return http_resp_from_controller_error(&ControllerError::pipeline_terminating()),
    };

    response
}

#[cfg(test)]
#[cfg(feature = "with-kafka")]
#[cfg(feature = "server")]
mod test_with_kafka {
    use super::{build_app, PrometheusMetrics, ServerState};
    use crate::{
        test::{
            generate_test_batches,
            http::{TestHttpReceiver, TestHttpSender},
            kafka::{BufferConsumer, KafkaResources, TestProducer},
            test_circuit, wait, TEST_LOGGER,
        },
        Controller, ControllerError, PipelineConfig,
    };
    use actix_web::{http::StatusCode, middleware::Logger, web::Data as WebData, App};
    use crossbeam::queue::SegQueue;
    use futures_util::StreamExt;
    use log::{error, LevelFilter};
    use proptest::{
        strategy::{Strategy, ValueTree},
        test_runner::TestRunner,
    };
    use serde_json::{self, json, Value as JsonValue};
    use std::{sync::Arc, thread::sleep, time::Duration};

    #[actix_web::test]
    async fn test_server() {
        // We cannot use proptest macros in `async` context, so generate
        // some random data manually.
        let mut runner = TestRunner::default();
        let data = generate_test_batches(100, 1000)
            .new_tree(&mut runner)
            .unwrap()
            .current();

        let _ = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);

        // Create topics.
        let kafka_resources = KafkaResources::create_topics(&[
            ("test_server_input_topic", 1),
            ("test_server_output_topic", 1),
        ]);

        // Create buffer consumer
        let buffer_consumer = BufferConsumer::new("test_server_output_topic");

        // Config string
        let config_str = r#"
name: test
inputs:
    test_input1:
        stream: test_input1
        transport:
            name: kafka
            config:
                bootstrap.servers: "localhost"
                auto.offset.reset: "earliest"
                topics: [test_server_input_topic]
                log_level: debug
        format:
            name: csv
outputs:
    test_output2:
        stream: test_output1
        transport:
            name: kafka
            config:
                bootstrap.servers: "localhost"
                topic: test_server_output_topic
                max_inflight_messages: 0
        format:
            name: csv
"#;

        // Create circuit
        println!("Creating circuit");
        let (circuit, catalog) = test_circuit(4);

        let errors = Arc::new(SegQueue::new());
        let errors_clone = errors.clone();

        let config: PipelineConfig = serde_yaml::from_str(config_str).unwrap();
        let controller = Controller::with_config(
            circuit,
            catalog,
            &config,
            Box::new(move |e| {
                error!("{e}");
                errors_clone.push(e);
            }) as Box<dyn Fn(ControllerError) + Send + Sync>,
        )
        .unwrap();

        // Create service
        println!("Creating HTTP server");

        let prometheus = PrometheusMetrics::new(&controller).unwrap();
        let state = WebData::new(ServerState::new(
            controller,
            prometheus,
            "metadata".to_string(),
            None,
        ));
        let server =
            actix_test::start(move || build_app(App::new().wrap(Logger::default()), state.clone()));

        // Write data to Kafka.
        println!("Send test data");
        let producer = TestProducer::new();
        producer.send_to_topic(&data, "test_server_input_topic");

        sleep(Duration::from_millis(2000));
        assert!(buffer_consumer.is_empty());

        // Start command; wait for data.
        println!("/start");
        let resp = server.get("/start").send().await.unwrap();
        assert!(resp.status().is_success());

        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        println!("/stats");
        let resp = server.get("/stats").send().await.unwrap();
        assert!(resp.status().is_success());

        println!("/metadata");
        let resp = server.get("/metadata").send().await.unwrap();
        assert!(resp.status().is_success());

        // Pause command; send more data, receive none.
        println!("/pause");
        let resp = server.get("/pause").send().await.unwrap();
        assert!(resp.status().is_success());
        sleep(Duration::from_millis(1000));

        producer.send_to_topic(&data, "test_server_input_topic");
        sleep(Duration::from_millis(2000));
        assert_eq!(buffer_consumer.len(), 0);

        // Start; wait for data
        println!("/start");
        let resp = server.get("/start").send().await.unwrap();
        assert!(resp.status().is_success());

        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        println!("Testing invalid input");
        producer.send_string("invalid\n", "test_server_input_topic");
        wait(|| errors.len() == 1, None);

        println!("Connecting to HTTP output endpoint");
        let mut resp1 = server.get("/egress/test_output1").send().await.unwrap();

        let mut resp2 = server.get("/egress/test_output1").send().await.unwrap();

        println!("Streaming test");
        let req = server.post("/ingress/test_input1");

        TestHttpSender::send_stream(req, &data).await;
        println!("data sent");

        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        TestHttpReceiver::wait_for_output_unordered(&mut resp1, &data).await;
        TestHttpReceiver::wait_for_output_unordered(&mut resp2, &data).await;

        let req = server.post("/ingress/test_input1");

        TestHttpSender::send_stream(req, &data).await;

        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        TestHttpReceiver::wait_for_output_unordered(&mut resp1, &data).await;
        TestHttpReceiver::wait_for_output_unordered(&mut resp2, &data).await;
        drop(resp1);
        drop(resp2);

        // Request quantiles.
        let mut quantiles_resp1 = server
            .get("/egress/test_output1?mode=snapshot&query=quantiles")
            .send()
            .await
            .unwrap();
        assert!(quantiles_resp1.status().is_success());
        let body = quantiles_resp1.body().await;
        // println!("Response: {body:?}");
        let body = serde_json::from_slice::<JsonValue>(&body.unwrap()).unwrap();
        println!("Quantiles: {body}");

        // Request quantiles for the input collection -- inputs must also behave as outputs.
        let mut input_quantiles = server
            .get("/egress/test_input1?mode=snapshot&query=quantiles")
            .send()
            .await
            .unwrap();
        assert!(input_quantiles.status().is_success());
        let body = input_quantiles.body().await;
        let body = serde_json::from_slice::<JsonValue>(&body.unwrap()).unwrap();
        println!("Input quantiles: {body}");

        // Request neighborhood snapshot.
        let mut hood_resp1 = server
            .get("/egress/test_output1?mode=snapshot&query=neighborhood")
            .send_json(
                &json!({"anchor": {"id": 1000, "b": true, "s": "foo"}, "before": 50, "after": 30}),
            )
            .await
            .unwrap();
        assert!(hood_resp1.status().is_success());
        let body = hood_resp1.body().await;
        // println!("Response: {body:?}");
        let body = serde_json::from_slice::<JsonValue>(&body.unwrap()).unwrap();
        println!("Neighborhood: {body}");

        // Request neighborhood snapshot: invalid request.
        let mut hood_inv_resp = server
            .get("/egress/test_output1?mode=snapshot&query=neighborhood")
            .send_json(
                &json!({"anchor": {"id": "string_instead_of_integer", "b": true, "s": "foo"}, "before": 50, "after": 30}),
            )
            .await
            .unwrap();
        assert_eq!(hood_inv_resp.status(), StatusCode::BAD_REQUEST);
        let body = hood_inv_resp.body().await;
        // println!("Response: {body:?}");
        let body = serde_json::from_slice::<JsonValue>(&body.unwrap()).unwrap();
        println!("Neighborhood: {body}");

        // Request neighborhood stream.
        let mut hood_resp2 = server
            .get("/egress/test_output1?mode=watch&query=neighborhood")
            .send_json(
                &json!({"anchor": {"id": 1000, "b": true, "s": "foo"}, "before": 50, "after": 30}),
            )
            .await
            .unwrap();
        assert!(hood_resp2.status().is_success());

        let bytes = hood_resp2.next().await.unwrap().unwrap();
        // println!("Response: {body:?}");
        let body = serde_json::from_slice::<JsonValue>(&bytes).unwrap();
        println!("Neighborhood: {body}");

        println!("/pause");
        let resp = server.get("/pause").send().await.unwrap();
        assert!(resp.status().is_success());
        sleep(Duration::from_millis(1000));

        // Shutdown
        println!("/shutdown");
        let resp = server.get("/shutdown").send().await.unwrap();
        // println!("Response: {resp:?}");
        assert!(resp.status().is_success());

        // Start after shutdown must fail.
        println!("/start");
        let resp = server.get("/start").send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        drop(buffer_consumer);
        drop(kafka_resources);
    }
}
