use crate::controller::CompletionToken;
use crate::format::{get_input_format, get_output_format};
use crate::server::metrics::{
    JsonFormatter, LabelStack, MetricsFormatter, MetricsWriter, PrometheusFormatter,
};
use crate::{
    adhoc::{adhoc_websocket, stream_adhoc_result},
    controller::ConnectorConfig,
    ensure_default_crypto_provider,
    transport::http::{
        HttpInputEndpoint, HttpInputTransport, HttpOutputEndpoint, HttpOutputTransport,
    },
    CircuitCatalog, Controller, ControllerError, FormatConfig, InputEndpointConfig, OutputEndpoint,
    OutputEndpointConfig, PipelineConfig, TransportInputEndpoint,
};
use crate::{dyn_event, Catalog};
use actix_web::body::MessageBody;
use actix_web::dev::Service;
use actix_web::{
    dev::{ServiceFactory, ServiceRequest},
    get,
    http::header,
    post, rt,
    web::{self, Data as WebData, Payload, Query},
    App, Error as ActixError, HttpRequest, HttpResponse, HttpServer, Responder,
};
use chrono::Utc;
use clap::Parser;
use colored::{ColoredString, Colorize};
use dbsp::{circuit::CircuitConfig, DBSPHandle};
use dbsp::{RootCircuit, Runtime};
use dyn_clone::DynClone;
use feldera_types::checkpoint::{
    CheckpointFailure, CheckpointResponse, CheckpointStatus, CheckpointSyncFailure,
    CheckpointSyncResponse, CheckpointSyncStatus,
};
use feldera_types::completion_token::{
    CompletionStatusArgs, CompletionStatusResponse, CompletionTokenResponse,
};
use feldera_types::query_params::{MetricsFormat, MetricsParameters};
use feldera_types::suspend::{SuspendError, SuspendableResponse};
use feldera_types::time_series::TimeSeries;
use feldera_types::{
    checkpoint::CheckpointMetadata,
    config::{default_max_batch_size, TransportConfig},
    transport::http::HttpInputConfig,
};
use feldera_types::{query::AdhocQueryArgs, transport::http::SERVER_PORT_FILE};
use futures_util::FutureExt;
use minitrace::collector::Config;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, DefaultHasher};
use std::path::PathBuf;
use std::{
    borrow::Cow,
    net::TcpListener,
    sync::{
        mpsc::{self, Sender as StdSender},
        Arc, Mutex, RwLock, Weak,
    },
    thread,
};
use tokio::{
    spawn,
    sync::{
        mpsc::{channel, Sender},
        oneshot,
    },
};
use tracing::{debug, error, info, info_span, trace, warn, Instrument, Level, Subscriber};
use tracing_subscriber::fmt::format::Format;
use tracing_subscriber::fmt::{FormatEvent, FormatFields};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

pub mod error;
pub mod metrics;

#[cfg(target_family = "unix")]
mod stack_overflow_backtrace;

pub use self::error::{ErrorResponse, PipelineError, MAX_REPORTED_PARSE_ERRORS};

/// Tracks the health of the pipeline.
///
/// Enables the server to report the state of the pipeline while it is
/// initializing, when it has failed to initialize, failed, or been suspended.
enum PipelinePhase {
    /// Initialization in progress.
    Initializing,

    /// Initialization has failed.
    InitializationError(Arc<ControllerError>),

    /// Initialization completed successfully.  Current state of the
    /// pipeline can be read using `controller.status()`.
    InitializationComplete,

    /// Pipeline encountered a fatal error.
    Failed(Arc<ControllerError>),

    /// Pipeline was successfully suspended to storage.
    Suspended,
}

/// Generate an appropriate error when `state.controller` is set to
/// `None`, which can mean that the pipeline is initializing, failed to
/// initialize, has been shut down or failed.
fn missing_controller_error(state: &ServerState) -> PipelineError {
    match &*state.phase.read().unwrap() {
        PipelinePhase::Initializing => PipelineError::Initializing,
        PipelinePhase::InitializationError(e) => {
            PipelineError::InitializationError { error: e.clone() }
        }
        PipelinePhase::InitializationComplete => PipelineError::Terminating,
        PipelinePhase::Failed(e) => PipelineError::ControllerError { error: e.clone() },
        PipelinePhase::Suspended => PipelineError::Suspended,
    }
}

#[derive(Clone, Debug, Default)]
struct CheckpointSyncState {
    status: CheckpointSyncStatus,
}

impl CheckpointSyncState {
    fn completed(&mut self, uuid: uuid::Uuid, result: Result<(), Arc<ControllerError>>) {
        match result {
            Ok(_) => self.status.success = Some(uuid),
            Err(e) => {
                self.status.failure = Some(CheckpointSyncFailure {
                    uuid,
                    error: e.to_string(),
                })
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
struct CheckpointState {
    /// Sequence number for the next checkpoint request.
    next_seq: u64,

    /// The UUID of the last checkpoint.
    last_checkpoint: Option<uuid::Uuid>,

    /// Status to report to user.
    status: CheckpointStatus,
}

impl CheckpointState {
    /// Returns the sequence number to use for the next checkpoint request.
    fn next_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    /// Updates the state for completion of the checkpoint request with sequence
    /// number `seq` with status `result`.
    fn completed(
        &mut self,
        seq: u64,
        result: Result<Option<CheckpointMetadata>, Arc<ControllerError>>,
    ) {
        match result {
            Ok(chk) => {
                self.last_checkpoint = chk.map(|c| c.uuid);
                if self.status.success.is_none_or(|success| success < seq) {
                    self.status.success = Some(seq);
                }
            }
            Err(error) => {
                if self
                    .status
                    .failure
                    .as_ref()
                    .is_none_or(|cf| cf.sequence_number < seq)
                {
                    self.status.failure = Some(CheckpointFailure {
                        sequence_number: seq,
                        error: error.to_string(),
                    });
                }
            }
        }
    }
}

struct ServerState {
    phase: Arc<RwLock<PipelinePhase>>,
    metadata: RwLock<String>,
    controller: RwLock<Option<Controller>>,
    checkpoint_state: Arc<Mutex<CheckpointState>>,
    sync_checkpoint_state: Arc<Mutex<CheckpointSyncState>>,
    /// Channel used to send a `kill` command to
    /// the self-destruct task when shutting down
    /// the server.
    terminate_sender: Option<Sender<()>>,
}

impl ServerState {
    fn new(terminate_sender: Option<Sender<()>>) -> Self {
        Self {
            phase: Arc::new(RwLock::new(PipelinePhase::Initializing)),
            metadata: RwLock::new(String::new()),
            controller: RwLock::new(None),
            checkpoint_state: Arc::new(Mutex::new(CheckpointState::default())),
            sync_checkpoint_state: Arc::new(Mutex::new(CheckpointSyncState::default())),
            terminate_sender,
        }
    }
}

#[derive(Parser, Debug, Default, Clone)]
#[command(author, version, about, long_about = None)]
pub struct ServerArgs {
    /// Pipeline configuration YAML file
    #[arg(short, long)]
    config_file: String,

    /// Pipeline metadata JSON file
    #[arg(short, long)]
    metadata_file: Option<String>,

    /// Persistent Storage location.
    ///
    /// If no default is specified, a temporary directory is used.
    #[arg(short, long)]
    pub(crate) storage_location: Option<PathBuf>,

    /// TCP bind address
    #[arg(short, long, default_value = "127.0.0.1")]
    bind_address: String,

    /// Run the server on this port if it is available. If the port is in
    /// use or no default port is specified, an unused TCP port is allocated
    /// automatically
    #[arg(short = 'p', long)]
    default_port: Option<u16>,

    /// Whether to enable TLS on the HTTP server.
    /// Iff true, is it allowed and required to set `https_tls_cert_path` and `https_tls_key_path`.
    #[arg(long, default_value_t = false)]
    pub enable_https: bool,

    /// Path to the TLS x509 certificate PEM file (e.g., `/path/to/tls.crt`).
    #[arg(long)]
    pub https_tls_cert_path: Option<String>,

    /// Path to the TLS key PEM file corresponding to the x509 certificate (e.g., `/path/to/tls.key`).
    #[arg(long)]
    pub https_tls_key_path: Option<String>,
}

pub trait CircuitBuilderFunc:
    FnOnce(&mut RootCircuit) -> Result<Catalog, anyhow::Error> + DynClone + Send + 'static
{
}

impl<F> CircuitBuilderFunc for F where
    F: FnOnce(&mut RootCircuit) -> Result<Catalog, anyhow::Error> + Clone + Send + 'static
{
}

dyn_clone::clone_trait_object! {CircuitBuilderFunc}

/// A version of Runtime::init_circuit optimized for use in compiler-generated code.
///
/// This function does not have any generic arguments, and so will be precompiled as part
/// of the adapters crate, pulling all scheduler code with it.  It reduces the size
/// of LLVM code for the main generated crate from 126K to 35K lines.
pub fn init_circuit(
    config: CircuitConfig,
    constructor: Box<dyn CircuitBuilderFunc>,
) -> Result<(DBSPHandle, Catalog), dbsp::Error> {
    Runtime::init_circuit(config, constructor)
}

pub type CircuitFactoryFunc = Box<
    dyn FnOnce(CircuitConfig) -> Result<(DBSPHandle, Box<dyn CircuitCatalog>), ControllerError>
        + Send
        + 'static,
>;

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
///   input/output stream catalog.
pub fn server_main<F>(circuit_factory: F) -> Result<(), ControllerError>
where
    F: FnOnce(CircuitConfig) -> Result<(DBSPHandle, Box<dyn CircuitCatalog>), ControllerError>
        + Send
        + 'static,
{
    let args = ServerArgs::try_parse().map_err(|e| ControllerError::cli_args_error(&e))?;

    run_server(args, Box::new(circuit_factory)).map_err(|e| {
        error!("{e}");
        e
    })
}

// We pass circuit_factory as a trait object to make sure that this function
// doesn't have any generics and gets generated, along with the entire web server
// as part of the adapters crate. This reduces the size of the generated code for
// the main crate by ~300L lines of LLVM code.
pub fn run_server(
    args: ServerArgs,
    circuit_factory: CircuitFactoryFunc,
) -> Result<(), ControllerError> {
    ensure_default_crypto_provider();

    let bind_address = args.bind_address.clone();
    let port = args.default_port.unwrap_or(0);
    let listener = TcpListener::bind((bind_address, port))
        .map_err(|e| ControllerError::io_error(format!("binding to TCP port {port}"), e))?;

    let port = listener
        .local_addr()
        .map_err(|e| {
            ControllerError::io_error(
                "retrieving local socket address of the TCP listener".to_string(),
                e,
            )
        })?
        .port();

    let (terminate_sender, mut terminate_receiver) = channel(1);

    let state = WebData::new(ServerState::new(Some(terminate_sender)));
    let state_clone = state.clone();

    // The bootstrap thread initialize the logger.
    // Use this channel to wait for the log to be ready, so that the first few
    // messages from the server don't get lost.
    let (loginit_sender, loginit_receiver) = mpsc::channel();
    let config = parse_config(&args.config_file).map_err(|e| {
        // Print to stderr until logging is initialized.
        eprintln!("{e}");
        e
    })?;

    // Install stack overflow handler early, before creating the controller and parsing DevTweaks.
    #[cfg(target_family = "unix")]
    if config
        .global
        .dev_tweaks
        .get("stack_overflow_backtrace")
        .cloned()
        == Some(serde_json::Value::Bool(true))
    {
        unsafe {
            use crate::server::stack_overflow_backtrace::enable_stack_overflow_backtrace_with_limit;

            // TODO: make count configurable.
            enable_stack_overflow_backtrace_with_limit(200);
        }
    }

    // Initialize the pipeline in a separate thread.  On success, this thread
    // will create a `Controller` instance and store it in `state.controller`.
    {
        let args = args.clone();
        let config = config.clone();
        thread::Builder::new()
            .name("pipeline-init".to_string())
            .spawn(move || bootstrap(args, config, circuit_factory, state_clone, loginit_sender))
            .expect("failed to spawn pipeline initialization thread");
        let _ = loginit_receiver.recv();
    }

    let workers = if let Some(http_workers) = config.global.http_workers {
        http_workers as usize
    } else {
        config.global.workers as usize
    };

    let server = HttpServer::new(move || {
        let state = state.clone();
        build_app(
            App::new().wrap_fn(|req, srv| {
                trace!("Request: {} {}", req.method(), req.path());
                srv.call(req).map(|res| {
                    match &res {
                        Ok(response) => {
                            let level = if response.status().is_success()
                                || response.status().is_redirection()
                                || response.status().is_informational()
                            {
                                Level::DEBUG
                            } else {
                                Level::ERROR
                            };
                            let req = response.request();
                            dyn_event!(
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
                })
            }),
            state,
        )
    })
    // The next two settings work around the issue that std::thread::available_parallelism()
    // which is what actix uses internally can't determine the number of threads available
    // in k8s (it will yield the number of cores on the node rather than the number of
    // cores assigned to the cgroup).
    // https://github.com/rust-lang/rust/issues/74479#issuecomment-717097590
    .workers(workers)
    .worker_max_blocking_threads(std::cmp::max(512 / workers, 1))
    // Set timeout for graceful shutdown of workers.
    // The default in actix is 30s. We may consider making this configurable.
    .shutdown_timeout(10);

    let server = if args.enable_https
        || args.https_tls_cert_path.is_some()
        || args.https_tls_key_path.is_some()
    {
        assert!(
            args.enable_https,
            "--enable-https is required to pass --https-tls-cert-path or --https-tls-key-path"
        );
        let https_tls_cert_path = args
            .https_tls_cert_path
            .as_ref()
            .expect("CLI argument --https-tls-cert-path is required");
        let https_tls_key_path = args
            .https_tls_key_path
            .as_ref()
            .expect("CLI argument --https-tls-key-path is required");

        // Load in certificate (public)
        let cert_chain = CertificateDer::pem_file_iter(https_tls_cert_path)
            .expect("HTTPS TLS certificate should be read")
            .flatten()
            .collect();

        // Load in key (private)
        let key_der =
            PrivateKeyDer::from_pem_file(https_tls_key_path).expect("HTTPS TLS key should be read");

        // Server configuration
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key_der)
            .expect("server configuration should be built");

        server
            .listen_rustls_0_23(listener, server_config)
            .map_err(|e| {
                ControllerError::io_error("binding HTTPS server to the listener".to_string(), e)
            })?
            .run()
    } else {
        server
            .listen(listener)
            .map_err(|e| {
                ControllerError::io_error("binding HTTP server to the listener".to_string(), e)
            })?
            .run()
    };

    rt::System::new().block_on(async {
        // Spawn a task that will shut down the server on `/kill`.
        let server_handle = server.handle();
        spawn(async move {
            terminate_receiver.recv().await;
            server_handle.stop(true).await
        });

        info!(
            "Started {} server on port {port}",
            if args.enable_https { "HTTPS" } else { "HTTP" }
        );

        // We don't want outside observers (e.g., the local runner) to observe a partially
        // written port file, so we write it to a temporary file first, and then rename the
        // temporary.
        let tmp_server_port_file = format!("{SERVER_PORT_FILE}.tmp");
        tokio::fs::write(&tmp_server_port_file, format!("{}\n", port))
            .await
            .map_err(|e| ControllerError::io_error("writing server port file".to_string(), e))?;
        tokio::fs::rename(&tmp_server_port_file, SERVER_PORT_FILE)
            .await
            .map_err(|e| ControllerError::io_error("renaming server port file".to_string(), e))?;
        server
            .await
            .map_err(|e| ControllerError::io_error("in the HTTP server".to_string(), e))
    })?;

    Ok(())
}

fn parse_config(config_file: &str) -> Result<PipelineConfig, ControllerError> {
    let yaml_config = std::fs::read(config_file).map_err(|e| {
        ControllerError::io_error(format!("reading configuration file '{}'", config_file), e)
    })?;

    let yaml_config = String::from_utf8(yaml_config).map_err(|e| {
        ControllerError::pipeline_config_parse_error(&format!(
            "invalid UTF8 string in configuration file '{}' ({e})",
            &config_file
        ))
    })?;

    // Still running without logger here.
    eprintln!("Pipeline configuration:\n{yaml_config}");

    // Deserialize the pipeline configuration
    serde_yaml::from_str(yaml_config.as_str())
        .map_err(|e| ControllerError::pipeline_config_parse_error(&e))
}

// Initialization thread function.
fn bootstrap(
    args: ServerArgs,
    config: PipelineConfig,
    circuit_factory: CircuitFactoryFunc,
    state: WebData<ServerState>,
    loginit_sender: StdSender<()>,
) {
    do_bootstrap(args, config, circuit_factory, &state, loginit_sender).unwrap_or_else(|e| {
        // Store error in `state.phase`, so that it can be
        // reported by the server.
        error!("Error initializing the pipeline: {e}.");
        *state.phase.write().unwrap() = PipelinePhase::InitializationError(Arc::new(e));
    })
}

/// True if the pipeline cannot operate after `error` and must be shut down.
fn is_fatal_controller_error(error: &ControllerError) -> bool {
    matches!(
        error,
        ControllerError::DbspError { .. } | ControllerError::DbspPanic
    )
}

/// Handle errors from the controller.
fn error_handler(state: &Weak<ServerState>, error: Arc<ControllerError>) {
    error!("{error}");

    let state = match state.upgrade() {
        None => return,
        Some(state) => state,
    };

    if is_fatal_controller_error(&error) {
        // Prepare to handle poisoned locks in the following code.

        if let Ok(mut controller) = state.controller.write() {
            if let Some(controller) = controller.take() {
                if let Ok(mut phase) = state.phase.write() {
                    *phase = PipelinePhase::Failed(error);
                }

                controller.initiate_stop();
            }
        }

        /*if let Some(sender) = &state.terminate_sender {
            let _ = sender.try_send(());
        }
        if let Err(e) = std::fs::remove_file(SERVER_PORT_FILE) {
            warn!("Failed to remove server port file: {e}");
        }*/
    }
}

struct PipelineFormat {
    pipeline_name: ColoredString,
    inner: Format,
}

impl PipelineFormat {
    pub fn new(pipeline_name: ColoredString) -> Self {
        Self {
            pipeline_name,
            inner: Format::default(),
        }
    }
}

impl<S, N> FormatEvent<S, N> for PipelineFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        write!(writer, "{} ", self.pipeline_name)?;
        self.inner.format_event(ctx, writer, event)
    }
}

/// Get the log filtering configuration.
fn get_env_filter(config: &PipelineConfig) -> EnvFilter {
    // The `RUST_LOG` environment variable takes precedence.  It isn't usually
    // set.
    if let Ok(env_filter) = EnvFilter::try_from_default_env() {
        return env_filter;
    }

    // Otherwise, take the configuration from the pipeline.
    if let Some(dirs) = &config.global.logging {
        match EnvFilter::try_new(dirs) {
            Ok(env_filter) => return env_filter,
            Err(error) => {
                // Write directly to stderr because logging isn't set up yet.
                eprintln!("Invalid pipeline `logging` configuration ({error}): {dirs}")
            }
        }
    }

    // Otherwise, fall back to `INFO`.
    EnvFilter::try_new("info").unwrap()
}

fn do_bootstrap(
    args: ServerArgs,
    config: PipelineConfig,
    circuit_factory: CircuitFactoryFunc,
    state: &WebData<ServerState>,
    loginit_sender: StdSender<()>,
) -> Result<(), ControllerError> {
    #[cfg(not(feature = "feldera-enterprise"))]
    if config.global.fault_tolerance.is_enabled() {
        return Err(ControllerError::EnterpriseFeature("fault tolerance"));
    }

    // Initializes the logger by setting its filter and template.
    let pipeline_name = format!("[{}]", config.name.clone().unwrap_or_default()).cyan();
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().event_format(PipelineFormat::new(pipeline_name)))
        .with(get_env_filter(&config))
        .try_init()
        .unwrap_or_else(|e| {
            // This happens in unit tests when another test has initialized logging.
            eprintln!("Failed to initialize logging: {e}.")
        });
    let _ = loginit_sender.send(());

    if config.global.tracing {
        use std::net::{SocketAddr, ToSocketAddrs};
        let socket_addrs: Vec<SocketAddr> = config
            .global
            .tracing_endpoint_jaeger
            .to_socket_addrs()
            .expect("Valid `tracing_endpoint_jaeger` value (e.g., localhost:6831)")
            .collect();
        let reporter = minitrace_jaeger::JaegerReporter::new(
            *socket_addrs
                .first()
                .expect("Valid `tracing_endpoint_jaeger` value (e.g., localhost:6831)"),
            config
                .name
                .clone()
                .unwrap_or("unknown pipeline".to_string()),
        )
        .unwrap();
        minitrace::set_reporter(reporter, Config::default());
    }

    *state.metadata.write().unwrap() = match &args.metadata_file {
        None => String::new(),
        Some(metadata_file) => {
            let meta = std::fs::read(metadata_file).map_err(|e| {
                ControllerError::io_error(format!("reading metadata file '{}'", metadata_file), e)
            })?;
            String::from_utf8(meta).map_err(|e| {
                ControllerError::pipeline_config_parse_error(&format!(
                    "invalid UTF8 string in the metadata file '{}' ({e})",
                    metadata_file
                ))
            })?
        }
    };

    let weak_state_ref = Arc::downgrade(state);
    let controller = Controller::with_config(
        circuit_factory,
        &config,
        Box::new(move |e| error_handler(&weak_state_ref, e))
            as Box<dyn Fn(Arc<ControllerError>) + Send + Sync>,
    )?;

    *state.controller.write().unwrap() = Some(controller);

    info!("Pipeline initialization complete");
    if let Some(runtime_override) = option_env!("FELDERA_RUNTIME_OVERRIDE") {
        warn!(
            "Pipeline runtime version was overridden and does not match platform version: {}",
            runtime_override
        );
    }
    *state.phase.write().unwrap() = PipelinePhase::InitializationComplete;

    Ok(())
}

fn build_app<T>(app: App<T>, state: WebData<ServerState>) -> App<T>
where
    T: ServiceFactory<ServiceRequest, Config = (), Error = ActixError, InitError = ()>,
{
    app.app_data(state)
        .route(
            "/",
            web::get().to(move || async {
                HttpResponse::Ok().body("<html><head><title>DBSP server</title></head></html>")
            }),
        )
        .service(checkpoint_sync)
        .service(start)
        .service(pause)
        .service(shutdown)
        .service(status)
        .service(suspendable)
        .service(completion_token)
        .service(completion_status)
        .service(query)
        .service(stats)
        .service(metrics_handler)
        .service(time_series)
        .service(metadata)
        .service(heap_profile)
        .service(dump_profile)
        .service(lir)
        .service(checkpoint)
        .service(checkpoint_status)
        .service(sync_checkpoint_status)
        .service(suspend)
        .service(input_endpoint)
        .service(output_endpoint)
        .service(pause_input_endpoint)
        .service(start_input_endpoint)
        .service(input_endpoint_status)
        .service(output_endpoint_status)
}

#[get("/start")]
async fn start(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.read().unwrap() {
        Some(controller) => {
            controller.start();
            Ok(HttpResponse::Ok().json("The pipeline is running"))
        }
        None => Err(missing_controller_error(&state)),
    }
}

#[get("/pause")]
async fn pause(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.read().unwrap() {
        Some(controller) => {
            controller.pause();
            Ok(HttpResponse::Ok().json("Pipeline paused"))
        }
        None => Err(missing_controller_error(&state)),
    }
}

/// Retrieve pipeline status.
///
/// The status is either `Paused`, `Running` or `Terminated`.
#[get("/status")]
async fn status(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.read().unwrap() {
        Some(controller) => {
            let status = controller.status().global_metrics.get_state();
            Ok(HttpResponse::Ok().json(status))
        }
        None => Err(missing_controller_error(&state)),
    }
}

/// Retrieve whether a pipeline is suspendable or not.
#[get("/suspendable")]
async fn suspendable(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.read().unwrap() {
        Some(controller) => {
            let suspend_error = controller.status().suspend_error.lock().unwrap().clone();

            let reasons = match suspend_error {
                Some(SuspendError::Permanent(errors)) => Some(errors.clone()),
                _ => None,
            };
            Ok(HttpResponse::Ok().json(SuspendableResponse::new(
                reasons.is_none(),
                reasons.unwrap_or_default(),
            )))
        }
        None => Err(missing_controller_error(&state)),
    }
}

fn request_is_websocket(request: &HttpRequest) -> bool {
    request
        .headers()
        .get(header::CONNECTION)
        .and_then(|val| val.to_str().ok())
        .is_some_and(|conn| conn.to_ascii_lowercase().contains("upgrade"))
        && request
            .headers()
            .get(header::UPGRADE)
            .and_then(|val| val.to_str().ok())
            .is_some_and(|upgrade| upgrade.eq_ignore_ascii_case("websocket"))
}

#[get("/query")]
async fn query(
    state: WebData<ServerState>,
    args: Query<AdhocQueryArgs>,
    request: HttpRequest,
    stream: web::Payload,
) -> impl Responder {
    let session_ctxt = {
        let controller = state.controller.read().unwrap();
        controller.as_ref().map(|c| c.session_context())
    };

    match session_ctxt.transpose()? {
        Some(session) => {
            if !request_is_websocket(&request) {
                stream_adhoc_result(args.into_inner(), session).await
            } else {
                adhoc_websocket(session, request, stream).await
            }
        }
        None => Err(missing_controller_error(&state)),
    }
}

#[get("/stats")]
async fn stats(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.read().unwrap() {
        Some(controller) => Ok(HttpResponse::Ok().json(controller.status())),
        None => Err(missing_controller_error(&state)),
    }
}

/// Retrieve circuit metrics.
#[get("/metrics")]
async fn metrics_handler(
    state: WebData<ServerState>,
    query_params: web::Query<MetricsParameters>,
) -> impl Responder {
    fn serialize_metrics<F>(controller: &Controller) -> String
    where
        F: MetricsFormatter,
    {
        let controller_status = controller.status();
        let mut metrics_writer = MetricsWriter::<F>::new();
        let labels = LabelStack::new();
        let labels = labels.with(
            "pipeline",
            controller_status
                .pipeline_config
                .name
                .as_ref()
                .map_or("unnamed", |s| s.as_str()),
        );
        controller.write_metrics(&mut metrics_writer, &labels, controller_status);
        metrics_writer.into_output()
    }

    match &*state.controller.read().unwrap() {
        Some(controller) => match &query_params.format {
            MetricsFormat::Prometheus => Ok(HttpResponse::Ok()
                .content_type(mime::TEXT_PLAIN)
                .body(serialize_metrics::<PrometheusFormatter>(controller))),
            MetricsFormat::Json => Ok(HttpResponse::Ok()
                .content_type(mime::APPLICATION_JSON)
                .body(serialize_metrics::<JsonFormatter>(controller))),
        },
        None => Err(missing_controller_error(&state)),
    }
}

/// Retrieve time series for basic statistics.
#[get("/time_series")]
async fn time_series(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.read().unwrap() {
        Some(controller) => {
            let time_series = TimeSeries {
                now: Utc::now(),
                samples: controller
                    .status()
                    .time_series
                    .lock()
                    .unwrap()
                    .iter()
                    .cloned()
                    .collect(),
            };
            Ok(HttpResponse::Ok().json(time_series))
        }
        None => Err(missing_controller_error(&state)),
    }
}

/// Returns static metadata associated with the circuit.  Can contain any
/// string, but the intention is to store information about the circuit (e.g.,
/// SQL code) in JSON format.
#[get("/metadata")]
async fn metadata(state: WebData<ServerState>) -> impl Responder {
    HttpResponse::Ok()
        .content_type(mime::APPLICATION_JSON)
        .body(state.metadata.read().unwrap().clone())
}

#[get("/heap_profile")]
async fn heap_profile() -> impl Responder {
    #[cfg(target_os = "linux")]
    {
        let mut prof_ctl = jemalloc_pprof::PROF_CTL.as_ref().unwrap().lock().await;
        if !prof_ctl.activated() {
            return Err(PipelineError::HeapProfilerError {
                error: "jemalloc profiling is disabled".to_string(),
            });
        }
        match prof_ctl.dump_pprof() {
            Ok(profile) => Ok(HttpResponse::Ok()
                .content_type("application/protobuf")
                .body(profile)),
            Err(e) => Err(PipelineError::HeapProfilerError {
                error: e.to_string(),
            }),
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err::<HttpResponse, PipelineError>(PipelineError::HeapProfilerError {
            error: "heap profiling is only supported on Linux".to_string(),
        })
    }
}

#[get("/dump_profile")]
async fn dump_profile(state: WebData<ServerState>) -> impl Responder {
    let (sender, receiver) = oneshot::channel();
    match &*state.controller.read().unwrap() {
        None => return Err(missing_controller_error(&state)),
        Some(controller) => {
            controller.start_graph_profile(Box::new(move |profile| {
                if sender.send(profile).is_err() {
                    error!("`/dump_profile` result could not be sent");
                }
            }));
        }
    };
    let profile = receiver.await.unwrap()?;

    Ok(HttpResponse::Ok()
        .insert_header(header::ContentType("application/zip".parse().unwrap()))
        .insert_header(header::ContentDisposition::attachment("profile.zip"))
        .body(profile.as_zip()))
}

/// Dump the low-level IR of the circuit.
#[get("/lir")]
async fn lir(state: WebData<ServerState>) -> impl Responder {
    let lir = match &*state.controller.read().unwrap() {
        None => return Err(missing_controller_error(&state)),
        Some(controller) => controller.lir().clone(),
    };

    Ok(HttpResponse::Ok()
        .insert_header(header::ContentType("application/zip".parse().unwrap()))
        .insert_header(header::ContentDisposition::attachment("lir.zip"))
        .body(lir.as_zip()))
}

#[post("/checkpoint/sync")]
async fn checkpoint_sync(state: WebData<ServerState>) -> Result<impl Responder, PipelineError> {
    let last_checkpoint = match &*state.controller.read().unwrap() {
        None => return Err(missing_controller_error(&state)),
        Some(controller) => {
            let Some(last_checkpoint) = ({
                let state = state.checkpoint_state.clone();
                let chk = state.lock().unwrap().last_checkpoint;
                chk
            }) else {
                return Ok(HttpResponse::BadRequest().json(ErrorResponse {
                    message: "no checkpoints found; make a POST request to `/checkpoint` to make a new checkpoint".to_string(),
                    error_code: "400".into(),
                    details: serde_json::Value::Null,
                }));
            };

            let state = state.sync_checkpoint_state.clone();
            controller.start_sync_checkpoint(
                last_checkpoint,
                Box::new(move |result| {
                    state.lock().unwrap().completed(last_checkpoint, result);
                }),
            );
            last_checkpoint
        }
    };

    Ok(HttpResponse::Accepted().json(CheckpointSyncResponse::new(last_checkpoint)))
}

/// Initiates a checkpoint and returns its sequence number.  The caller may poll
/// `/checkpoint_status` to determine when the checkpoint completes.
#[post("/checkpoint")]
async fn checkpoint(state: WebData<ServerState>) -> impl Responder {
    let seq = match &*state.controller.read().unwrap() {
        None => return Err(missing_controller_error(&state)),
        Some(controller) => {
            let state = state.checkpoint_state.clone();
            let seq = state.lock().unwrap().next_seq();
            controller.start_checkpoint(Box::new(move |result| {
                state
                    .lock()
                    .unwrap()
                    .completed(seq, result.map(|c| c.circuit));
            }));
            seq
        }
    };
    Ok(HttpResponse::Ok().json(CheckpointResponse::new(seq)))
}

#[get("/checkpoint_status")]
async fn checkpoint_status(state: WebData<ServerState>) -> impl Responder {
    HttpResponse::Ok().json(state.checkpoint_state.lock().unwrap().status.clone())
}

#[get("/checkpoint/sync_status")]
async fn sync_checkpoint_status(state: WebData<ServerState>) -> impl Responder {
    HttpResponse::Ok().json(state.sync_checkpoint_state.lock().unwrap().status.clone())
}

/// Suspends the pipeline (but only a later call to `/shutdown` will shut down
/// the webserver or terminate the process).
///
/// This implementation is designed to be idempotent, so that any number of
/// suspend requests act like just one.
#[post("/suspend")]
async fn suspend(state: WebData<ServerState>) -> Result<impl Responder, PipelineError> {
    fn success() -> Result<impl Responder, PipelineError> {
        Ok(HttpResponse::Ok().json("Pipeline suspended"))
    }

    let receiver = match &*state.controller.read().unwrap() {
        Some(controller) => {
            let (sender, receiver) = oneshot::channel();
            let phase = state.phase.clone();
            controller.start_suspend(Box::new(move |suspend| {
                if suspend.is_ok() {
                    *phase.write().unwrap() = PipelinePhase::Suspended;
                }
                if sender.send(suspend).is_err() {
                    error!("`/suspend` result could not be sent");
                }
            }));
            receiver
        }
        None => {
            match missing_controller_error(&state) {
                PipelineError::Suspended => {
                    // Ensure idempotence.
                    return success();
                }
                other => {
                    return Err(other);
                }
            }
        }
    };

    let result = receiver.await.unwrap();

    // If the pipeline is already suspended, ignore the error. The controller
    // can return a ControllerExit error when flushing any remaining suspend requests after
    // the first successful suspend operation.
    if !matches!(*state.phase.read().unwrap(), PipelinePhase::Suspended) {
        result?;
    }

    let Some(controller) = state.controller.write().unwrap().take() else {
        match missing_controller_error(&state) {
            PipelineError::Suspended => {
                // Ensure idempotence.
                return success();
            }
            other => return Err(other),
        }
    };

    controller.stop()?;
    success()
}

#[get("/shutdown")]
async fn shutdown(state: WebData<ServerState>) -> impl Responder {
    do_shutdown(state).await
}

async fn do_shutdown(state: WebData<ServerState>) -> Result<impl Responder, PipelineError> {
    let retval = if let Some(controller) = state.controller.write().unwrap().take() {
        controller.stop()?;
        Ok(HttpResponse::Ok().json("Pipeline terminated"))
    } else if let PipelinePhase::Initializing = &*state.phase.read().unwrap() {
        return Err(PipelineError::Initializing);
    } else {
        Ok(HttpResponse::Ok().json("Pipeline already terminated"))
    };

    if let Some(sender) = &state.terminate_sender {
        let _ = sender.send(()).await;
    }
    if let Err(e) = tokio::fs::remove_file(SERVER_PORT_FILE).await {
        warn!("Failed to remove server port file: {e}");
    }

    retval
}

#[derive(Debug, Deserialize)]
struct IngressArgs {
    #[serde(default = "HttpInputTransport::default_format")]
    format: String,
    /// Push data to the pipeline even if the pipeline is in a paused state.
    #[serde(default)]
    force: bool,
}

/// Create a new HTTP input endpoint.
async fn create_http_input_endpoint(
    state: &WebData<ServerState>,
    format: FormatConfig,
    table_name: String,
    endpoint_name: String,
) -> Result<HttpInputEndpoint, PipelineError> {
    let config = HttpInputConfig {
        name: endpoint_name.clone(),
    };

    let endpoint = HttpInputEndpoint::new(config.clone());
    // Create endpoint config.
    let config = InputEndpointConfig {
        stream: Cow::from(table_name),
        connector_config: ConnectorConfig {
            transport: TransportConfig::HttpInput(config),
            format: Some(format),
            index: None,
            output_buffer_config: Default::default(),
            max_batch_size: default_max_batch_size(),
            max_queued_records: HttpInputTransport::default_max_buffered_records(),
            paused: false,
            labels: vec![],
            start_after: None,
        },
    };

    // Connect endpoint.
    let _endpoint_id = match &*state.controller.read().unwrap() {
        Some(controller) => {
            if controller.register_api_connection().is_err() {
                return Err(PipelineError::ApiConnectionLimit);
            }

            match controller.add_input_endpoint(
                &endpoint_name,
                config,
                Box::new(endpoint.clone()) as Box<dyn TransportInputEndpoint>,
                None,
            ) {
                Ok(endpoint_id) => endpoint_id,
                Err(e) => {
                    controller.unregister_api_connection();
                    debug!("Failed to create API endpoint: '{e}'");
                    Err(e)?
                }
            }
        }
        None => {
            return Err(missing_controller_error(state));
        }
    };

    Ok(endpoint)
}

#[post("/ingress/{table_name}")]
async fn input_endpoint(
    state: WebData<ServerState>,
    req: HttpRequest,
    args: Query<IngressArgs>,
    payload: Payload,
) -> impl Responder {
    thread_local! {
        static TABLE_ENDPOINTS: RefCell<HashMap<(String, FormatConfig), HttpInputEndpoint, BuildHasherDefault<DefaultHasher>>> = const {
            RefCell::new(HashMap::with_hasher(BuildHasherDefault::new()))
        };
    }
    debug!("{req:?}");

    let table_name = match req.match_info().get("table_name") {
        None => {
            return Err(PipelineError::MissingUrlEncodedParam {
                param: "table_name",
            });
        }
        Some(table_name) => table_name.to_string(),
    };

    // Generate endpoint name.
    let endpoint_name = format!("api-ingress-{table_name}-{}", Uuid::new_v4());
    let format = parser_config_from_http_request(&endpoint_name, &args.format, &req)?;

    let cached_endpoint = TABLE_ENDPOINTS.with(|endpoints| {
        endpoints
            .borrow()
            .get(&(table_name.clone(), format.clone()))
            .cloned()
    });
    let endpoint = match cached_endpoint {
        Some(endpoint) => endpoint,
        None => {
            let endpoint = create_http_input_endpoint(
                &state,
                format.clone(),
                table_name.clone(),
                endpoint_name.clone(),
            )
            .await?;
            TABLE_ENDPOINTS.with_borrow_mut(|endpoints| {
                endpoints.insert((table_name, format), endpoint.clone())
            });
            endpoint
        }
    };

    // Call endpoint to complete request.
    endpoint
        .complete_request(payload, args.force)
        .instrument(info_span!("http_input"))
        .await?;

    match &*state.controller.read().unwrap() {
        None => Err(missing_controller_error(&state)),
        Some(controller) => {
            let token = controller.completion_token(endpoint.name())?.encode();
            Ok(HttpResponse::Ok().json(CompletionTokenResponse::new(token)))
        }
    }
}

/// Create an instance of `FormatConfig` from format name and
/// HTTP request using the `InputFormat::config_from_http_request` method.
pub fn parser_config_from_http_request(
    endpoint_name: &str,
    format_name: &str,
    request: &HttpRequest,
) -> Result<FormatConfig, ControllerError> {
    let format = get_input_format(format_name)
        .ok_or_else(|| ControllerError::unknown_input_format(endpoint_name, format_name))?;

    let config = format.config_from_http_request(endpoint_name, request)?;

    // Convert config to YAML format.
    // FIXME: this is hacky. Perhaps we can parameterize `FormatConfig` with the
    // exact type stored in the `config` field, so it can be either YAML or a
    // strongly typed format-specific config.
    Ok(FormatConfig {
        name: Cow::from(format_name.to_string()),
        config: serde_yaml::to_value(config)
            .map_err(|e| ControllerError::parser_config_parse_error(endpoint_name, &e, ""))?,
    })
}

/// Create an instance of `FormatConfig` from format name and
/// HTTP request using the `InputFormat::config_from_http_request` method.
pub fn encoder_config_from_http_request(
    endpoint_name: &str,
    format_name: &str,
    request: &HttpRequest,
) -> Result<FormatConfig, ControllerError> {
    let format = get_output_format(format_name)
        .ok_or_else(|| ControllerError::unknown_output_format(endpoint_name, format_name))?;

    let config = format.config_from_http_request(endpoint_name, request)?;

    Ok(FormatConfig {
        name: Cow::from(format_name.to_string()),
        config: serde_yaml::to_value(config)
            .map_err(|e| ControllerError::encoder_config_parse_error(endpoint_name, &e, ""))?,
    })
}

/// URL-encoded arguments to the `/egress` endpoint.
#[derive(Debug, Deserialize)]
struct EgressArgs {
    /// Apply backpressure on the pipeline when the HTTP client cannot receive
    /// data fast enough.
    ///
    /// When this flag is set to false (the default), the HTTP connector drops data
    /// chunks if the client is not keeping up with its output.  This prevents
    /// a slow HTTP client from slowing down the entire pipeline.
    ///
    /// When the flag is set to true, the connector waits for the client to receive
    /// each chunk and blocks the pipeline if the client cannot keep up.
    #[serde(default)]
    backpressure: bool,

    /// Data format used to encode the output of the query, e.g., 'csv',
    /// 'json' etc.
    #[serde(default = "HttpOutputTransport::default_format")]
    format: String,
}

#[post("/egress/{table_name}")]
async fn output_endpoint(
    state: WebData<ServerState>,
    req: HttpRequest,
    args: Query<EgressArgs>,
) -> impl Responder {
    debug!("/egress request:{req:?}");

    let state = state.into_inner();

    let table_name = match req.match_info().get("table_name") {
        None => {
            return Err(PipelineError::MissingUrlEncodedParam {
                param: "table_name",
            });
        }
        Some(table_name) => table_name.to_string(),
    };

    // Generate endpoint name depending on the query and output mode.
    let endpoint_name = format!("api-{}-{table_name}", Uuid::new_v4());

    // debug!("Endpoint name: '{endpoint_name}'");

    // Create HTTP endpoint.
    let endpoint = HttpOutputEndpoint::new(&endpoint_name, &args.format, args.backpressure);

    // Create endpoint config.
    let config = OutputEndpointConfig {
        stream: Cow::from(table_name),
        connector_config: ConnectorConfig {
            transport: HttpOutputTransport::config(),
            format: Some(encoder_config_from_http_request(
                &endpoint_name,
                &args.format,
                &req,
            )?),
            index: None,
            output_buffer_config: Default::default(),
            max_batch_size: default_max_batch_size(),
            max_queued_records: HttpOutputTransport::default_max_buffered_records(),
            paused: false,
            labels: vec![],
            start_after: None,
        },
    };

    // Connect endpoint.
    let response = match &*state.controller.read().unwrap() {
        Some(controller) => {
            if controller.register_api_connection().is_err() {
                return Err(PipelineError::ApiConnectionLimit);
            }

            let endpoint_id = match controller.add_output_endpoint(
                &endpoint_name,
                &config,
                Box::new(endpoint.clone()) as Box<dyn OutputEndpoint>,
            ) {
                Ok(endpoint_id) => endpoint_id,
                Err(e) => {
                    controller.unregister_api_connection();
                    Err(e)?
                }
            };

            // We need to pass a callback to `request` to disconnect the endpoint when the
            // request completes.  Use a downgraded reference to `state`, so
            // this closure doesn't prevent the controller from shutting down.
            let weak_state = Arc::downgrade(&state);

            // Call endpoint to create a response with a streaming body, which will be
            // evaluated after we return the response object to actix.
            endpoint.request(Box::new(move || {
                // Delete endpoint on completion/error.
                // We don't control the lifetime of the response object after
                // returning it to actix, so the only way to run cleanup code
                // when the HTTP request terminates is to piggyback on the
                // destructor.
                if let Some(state) = weak_state.upgrade() {
                    // This code will be invoked from `drop`, which means that
                    // it can run as part of a panic handler, so we need to
                    // handle a poisoned lock without causing a nested panic.
                    if let Ok(guard) = state.controller.read() {
                        if let Some(controller) = guard.as_ref() {
                            controller.disconnect_output(&endpoint_id);
                            controller.unregister_api_connection();
                        }
                    }
                }
            }))
        }
        None => return Err(missing_controller_error(&state)),
    };

    Ok(response)
}

/// This service journals the paused state, but it does not wait for the journal
/// record to commit before it returns success, so there is a small race.
#[get("/input_endpoints/{endpoint_name}/pause")]
async fn pause_input_endpoint(
    state: WebData<ServerState>,
    path: web::Path<String>,
) -> impl Responder {
    let endpoint_name = path.into_inner();

    match &*state.controller.read().unwrap() {
        Some(controller) => controller.pause_input_endpoint(&endpoint_name)?,
        None => {
            return Err(missing_controller_error(&state));
        }
    };

    Ok(HttpResponse::Ok())
}

#[get("/input_endpoints/{endpoint_name}/stats")]
async fn input_endpoint_status(
    state: WebData<ServerState>,
    path: web::Path<String>,
) -> impl Responder {
    let endpoint_name = path.into_inner();

    match &*state.controller.read().unwrap() {
        Some(controller) => {
            let ep_stats = controller.input_endpoint_status(&endpoint_name)?;
            Ok(HttpResponse::Ok().json(ep_stats))
        }
        None => Err(missing_controller_error(&state)),
    }
}

#[get("/output_endpoints/{endpoint_name}/stats")]
async fn output_endpoint_status(
    state: WebData<ServerState>,
    path: web::Path<String>,
) -> impl Responder {
    let endpoint_name = path.into_inner();

    match &*state.controller.read().unwrap() {
        Some(controller) => {
            let ep_stats = controller.output_endpoint_status(&endpoint_name)?;
            Ok(HttpResponse::Ok().json(ep_stats))
        }
        None => Err(missing_controller_error(&state)),
    }
}

/// This service journals the paused state, but it does not wait for the journal
/// record to commit before it returns success, so there is a small race.
#[get("/input_endpoints/{endpoint_name}/start")]
async fn start_input_endpoint(
    state: WebData<ServerState>,
    path: web::Path<String>,
) -> impl Responder {
    let endpoint_name = path.into_inner();

    match &*state.controller.read().unwrap() {
        Some(controller) => controller.start_input_endpoint(&endpoint_name)?,
        None => {
            return Err(missing_controller_error(&state));
        }
    };

    Ok(HttpResponse::Ok())
}

/// Generate a completion token for the endpoint.
#[get("/input_endpoints/{endpoint_name}/completion_token")]
async fn completion_token(state: WebData<ServerState>, path: web::Path<String>) -> impl Responder {
    let endpoint_name = path.into_inner();

    match &*state.controller.read().unwrap() {
        Some(controller) => {
            let token = controller.completion_token(&endpoint_name)?.encode();
            let response = CompletionTokenResponse::new(token);
            Ok(HttpResponse::Ok().json(response))
        }
        None => Err(missing_controller_error(&state)),
    }
}

/// Check the status of a completion token.
#[get("/completion_status")]
async fn completion_status(
    state: WebData<ServerState>,
    args: Query<CompletionStatusArgs>,
) -> impl Responder {
    match &*state.controller.read().unwrap() {
        Some(controller) => {
            let token = CompletionToken::decode(&args.into_inner().token).map_err(|e| {
                PipelineError::InvalidParam {
                    error: format!("invalid completion token: {e}"),
                }
            })?;
            if controller.completion_status(&token)? {
                Ok(HttpResponse::Ok().json(CompletionStatusResponse::complete()))
            } else {
                Ok(HttpResponse::Accepted().json(CompletionStatusResponse::inprogress()))
            }
        }
        None => Err(missing_controller_error(&state)),
    }
}

#[cfg(test)]
#[cfg(feature = "with-kafka")]
mod test_with_kafka {
    use super::{bootstrap, build_app, parse_config, ServerArgs, ServerState};
    use crate::{
        controller::MAX_API_CONNECTIONS,
        ensure_default_crypto_provider,
        test::{
            async_wait, generate_test_batches,
            http::{TestHttpReceiver, TestHttpSender},
            kafka::{BufferConsumer, KafkaResources, TestProducer},
            test_circuit, TestStruct,
        },
    };
    use actix_test::TestServer;
    use actix_web::{http::StatusCode, middleware::Logger, web::Data as WebData, App};
    use feldera_types::completion_token::{
        CompletionStatus, CompletionStatusResponse, CompletionTokenResponse,
    };
    use proptest::{
        strategy::{Strategy, ValueTree},
        test_runner::TestRunner,
    };
    use serde_json::{self, Value as JsonValue};
    use std::{
        io::Write,
        thread,
        thread::sleep,
        time::{Duration, Instant},
    };
    use tempfile::NamedTempFile;

    async fn print_stats(server: &TestServer) {
        let stats = serde_json::to_string_pretty(
            &server
                .get("/stats")
                .send()
                .await
                .unwrap()
                .json::<JsonValue>()
                .await
                .unwrap(),
        )
        .unwrap();

        println!("{stats}")
    }

    #[actix_web::test]
    async fn test_server() {
        ensure_default_crypto_provider();

        // We cannot use proptest macros in `async` context, so generate
        // some random data manually.
        let mut runner = TestRunner::default();
        let data = generate_test_batches(0, 100, 1000)
            .new_tree(&mut runner)
            .unwrap()
            .current();

        //let _ = log::set_logger(&TEST_LOGGER);
        //log::set_max_level(LevelFilter::Debug);

        // Create topics.
        let kafka_resources = KafkaResources::create_topics(&[
            ("test_server_input_topic", 1),
            ("test_server_output_topic", 1),
        ]);

        // Create buffer consumer
        let buffer_consumer = BufferConsumer::new("test_server_output_topic", "csv", "", None);

        // Config string
        let config_str = r#"
name: test
inputs:
    test_input1:
        stream: test_input1
        paused: true
        transport:
            name: kafka_input
            config:
                auto.offset.reset: "earliest"
                topics: [test_server_input_topic]
                log_level: debug
        format:
            name: csv
outputs:
    test_output2:
        stream: test_output1
        transport:
            name: kafka_output
            config:
                topic: test_server_output_topic
        format:
            name: csv
"#;

        let mut config_file = NamedTempFile::new().unwrap();
        config_file.write_all(config_str.as_bytes()).unwrap();

        println!("Creating HTTP server");

        let state = WebData::new(ServerState::new(None));
        let state_clone = state.clone();

        let args = ServerArgs {
            config_file: config_file.path().display().to_string(),
            metadata_file: None,
            bind_address: "127.0.0.1".to_string(),
            default_port: None,
            storage_location: None,
            enable_https: false,
            https_tls_cert_path: None,
            https_tls_key_path: None,
        };

        let config = parse_config(&args.config_file).unwrap();
        thread::spawn(move || {
            bootstrap(
                args,
                config,
                Box::new(|workers| {
                    Ok(test_circuit::<TestStruct>(
                        workers,
                        &TestStruct::schema(),
                        &[None],
                    ))
                }),
                state_clone,
                std::sync::mpsc::channel().0,
            )
        });

        let server =
            actix_test::start(move || build_app(App::new().wrap(Logger::default()), state.clone()));

        let start = Instant::now();
        while server.get("/stats").send().await.unwrap().status() == StatusCode::SERVICE_UNAVAILABLE
        {
            assert!(start.elapsed() < Duration::from_millis(20_000));
            sleep(Duration::from_millis(200));
        }

        // Write data to Kafka.
        println!("Send test data");
        let producer = TestProducer::new();
        producer.send_to_topic(&data, "test_server_input_topic");

        sleep(Duration::from_millis(2000));
        assert!(buffer_consumer.is_empty());

        // Start pipeline.
        println!("/start");
        let resp = server.get("/start").send().await.unwrap();
        assert!(resp.status().is_success());

        sleep(Duration::from_millis(3000));

        // Unpause input endpoint.
        println!("/input_endpoints/test_input1/start");
        let resp = server
            .get("/input_endpoints/test_input1/start")
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());

        // Wait for data.
        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        println!("/stats");
        let resp = server.get("/stats").send().await.unwrap();
        assert!(resp.status().is_success());

        println!("/metadata");
        let resp = server.get("/metadata").send().await.unwrap();
        assert!(resp.status().is_success());

        // Pause input endpoint.
        println!("/input_endpoints/test_input1/pause");
        let resp = server
            .get("/input_endpoints/test_input1/pause")
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());

        // Pause pipeline.
        println!("/pause");
        let resp = server.get("/pause").send().await.unwrap();
        assert!(resp.status().is_success());
        sleep(Duration::from_millis(1000));

        // Send more data, receive none
        producer.send_to_topic(&data, "test_server_input_topic");
        sleep(Duration::from_millis(2000));
        assert_eq!(buffer_consumer.len(), 0);

        // Start pipeline; still no data because the endpoint is paused.
        println!("/start");
        let resp = server.get("/start").send().await.unwrap();
        assert!(resp.status().is_success());

        sleep(Duration::from_millis(2000));
        assert_eq!(buffer_consumer.len(), 0);

        // Resume input endpoint, receive data.
        println!("/input_endpoints/test_input1/start");
        let resp = server
            .get("/input_endpoints/test_input1/start")
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());

        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        println!("Testing invalid input");
        producer.send_string("invalid\n", "test_server_input_topic");
        loop {
            let stats = server
                .get("/stats")
                .send()
                .await
                .unwrap()
                .json::<JsonValue>()
                .await
                .unwrap();
            // println!("stats: {stats:#}");
            let num_errors = stats.get("inputs").unwrap().as_array().unwrap()[0]
                .get("metrics")
                .unwrap()
                .get("num_parse_errors")
                .unwrap()
                .as_u64()
                .unwrap();
            if num_errors == 1 {
                break;
            }
        }

        // Make sure that HTTP connections get dropped on client disconnect
        // (see comment in `HttpOutputEndpoint::request`).  We create 2x the
        // number of supported simultaneous API connections and drop the client
        // side instantly, which should cause the server side to close within
        // 6 seconds.  If everything works as intended, this should _not_
        // trigger the API connection limit error.
        for _ in 0..2 * MAX_API_CONNECTIONS {
            assert!(server
                .post("/egress/test_output1")
                .send()
                .await
                .unwrap()
                .status()
                .is_success());
            sleep(Duration::from_millis(150));
        }

        println!("Connecting to HTTP output endpoint");
        let mut resp1 = server
            .post("/egress/test_output1?backpressure=true")
            .send()
            .await
            .unwrap();

        let mut resp2 = server
            .post("/egress/test_output1?backpressure=true")
            .send()
            .await
            .unwrap();

        println!("Streaming test");
        let req = server.post("/ingress/test_input1");

        TestHttpSender::send_stream(req, &data).await;
        println!("data sent");

        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        TestHttpReceiver::wait_for_output_unordered(&mut resp1, &data).await;
        TestHttpReceiver::wait_for_output_unordered(&mut resp2, &data).await;

        // Force-push data in paused state.
        println!("/pause");
        let resp = server.get("/pause").send().await.unwrap();
        assert!(resp.status().is_success());
        sleep(Duration::from_millis(1000));

        println!("Force-push data via HTTP");
        let req = server.post("/ingress/test_input1?force=true");

        let CompletionTokenResponse { token } =
            TestHttpSender::send_stream_deserialize_resp::<CompletionTokenResponse>(req, &data)
                .await;
        println!("completion token: {token}");

        tokio::join!(
            // Wait for completion.
            async {
                async_wait(
                    || async {
                        print_stats(&server).await;

                        let resp = server
                            .get(format!("/completion_status?token={token}"))
                            .send()
                            .await
                            .unwrap()
                            .body()
                            .await
                            .unwrap();
                        let CompletionStatusResponse { status } =
                            serde_json::from_slice(&resp).unwrap();
                        println!("completion status: {status:?}");

                        // println!("stats {}", stats.to_str_lossy());
                        status == CompletionStatus::Complete
                    },
                    20_000,
                )
                .await
                .unwrap()
            },
            // In parallel, run the HTTP client to receive outputs from the pipeline, otherwise the
            // HTTP output connector can get stuck, and the /completion_status check above will timeout.
            async {
                TestHttpReceiver::wait_for_output_unordered(&mut resp1, &data).await;
                TestHttpReceiver::wait_for_output_unordered(&mut resp2, &data).await;
            }
        );

        drop(resp1);
        drop(resp2);

        // Even though we checked completion status of the token, it only means that the connector
        // has sent data to Kafka, not that it has been received by the consumer.
        buffer_consumer.wait_for_output_unordered(&data);
        print_stats(&server).await;

        buffer_consumer.clear();

        println!("/start");
        let resp = server.get("/start").send().await.unwrap();
        assert!(resp.status().is_success());

        sleep(Duration::from_millis(5000));

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
        assert_eq!(resp.status(), StatusCode::GONE);

        drop(buffer_consumer);
        drop(kafka_resources);
    }
}
