use crate::controller::{CompletionToken, ControllerBuilder};
use crate::format::{get_input_format, get_output_format};
use crate::server::metrics::{
    JsonFormatter, LabelStack, MetricsFormatter, MetricsWriter, PrometheusFormatter,
};
use crate::util::{RateLimitCheckResult, TokenBucketRateLimiter};
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
use actix_web::http::KeepAlive;
use actix_web::{
    dev::{ServiceFactory, ServiceRequest},
    get,
    http::header,
    http::StatusCode,
    post, rt,
    web::{self, Data as WebData, Payload, Query},
    App, Error as ActixError, HttpRequest, HttpResponse, HttpServer, Responder, ResponseError,
};
use async_stream;
use chrono::Utc;
use clap::Parser;
use colored::Colorize;
use dbsp::{circuit::CircuitConfig, DBSPHandle};
use dbsp::{RootCircuit, Runtime};
use dyn_clone::DynClone;
use feldera_adapterlib::PipelineState;
use feldera_observability as observability;
use feldera_observability::json_logging::init_pipeline_logging_with_id;
use feldera_storage::{StorageBackend, StoragePath};
use feldera_types::adapter_stats::{
    EndpointErrorStats, InputEndpointErrorMetrics, OutputEndpointErrorMetrics,
    PipelineStatsErrorsResponse,
};
use feldera_types::checkpoint::{
    CheckpointFailure, CheckpointResponse, CheckpointStatus, CheckpointSyncFailure,
    CheckpointSyncResponse, CheckpointSyncStatus,
};
use feldera_types::completion_token::{
    CompletionStatusArgs, CompletionStatusResponse, CompletionTokenResponse,
};
use feldera_types::constants::STATUS_FILE;
use feldera_types::pipeline_diff::PipelineDiff;
use feldera_types::query_params::{ActivateParams, MetricsFormat, MetricsParameters};
use feldera_types::runtime_status::{
    BootstrapPolicy, ExtendedRuntimeStatus, ExtendedRuntimeStatusError, RuntimeDesiredStatus,
    RuntimeStatus,
};
use feldera_types::suspend::{SuspendError, SuspendableResponse};
use feldera_types::time_series::TimeSeries;
use feldera_types::{
    checkpoint::CheckpointMetadata,
    config::{default_max_batch_size, TransportConfig},
    transport::http::HttpInputConfig,
};
use feldera_types::{query::AdhocQueryArgs, transport::http::SERVER_PORT_FILE};
use futures_util::FutureExt;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::hash::{BuildHasherDefault, DefaultHasher};
use std::io::ErrorKind;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{
    borrow::Cow,
    net::TcpListener,
    sync::{atomic::Ordering, Arc, Mutex, Weak},
    thread,
};
use tokio::spawn;
use tokio::sync::Notify;
use tokio::task::spawn_blocking;
use tokio::time::sleep;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};
use tracing::{debug, error, info, info_span, warn, Instrument, Level};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

pub mod error;
pub mod metrics;

#[cfg(target_family = "unix")]
mod stack_overflow_backtrace;

pub use self::error::{ErrorResponse, PipelineError, MAX_REPORTED_PARSE_ERRORS};

#[derive(Debug, Clone, Default)]
pub enum InitializationState {
    #[default]
    Starting,
    DownloadingCheckpoint,
    Standby,
    AwaitingApproval(Box<PipelineDiff>),
}

/// Tracks the health of the pipeline.
///
/// Enables the server to report the state of the pipeline while it is
/// initializing, when it has failed to initialize, failed, or been suspended.
#[derive(Clone)]
pub enum PipelinePhase {
    /// Initialization in progress.
    Initializing(InitializationState),
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

impl PipelinePhase {
    fn is_awaiting_approval(&self) -> bool {
        matches!(
            self,
            PipelinePhase::Initializing(InitializationState::AwaitingApproval(_))
        )
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

pub(crate) struct ServerState {
    /// The other locks in this structure nest inside `desired_status`.
    desired_status: Mutex<RuntimeDesiredStatus>,

    bootstrap_policy: Mutex<BootstrapPolicy>,

    /// Notified when `desired_status` changes.
    desired_status_change: Arc<Notify>,

    /// `phase` nests inside `controller`.
    ///
    /// Use `controller()`, `take_controller()`, `set_controller()` to access.
    ///
    /// This lock is only held momentarily.
    controller: Mutex<Option<Controller>>,

    /// Leaf lock (no more locks may be taken while holding it).
    ///
    /// Use `phase()` and `set_phase()` to access.
    ///
    /// This lock is only held momentarily.
    phase: Mutex<PipelinePhase>,

    /// Leaf lock.
    checkpoint_state: Mutex<CheckpointState>,

    /// Leaf lock.
    sync_checkpoint_state: Mutex<CheckpointSyncState>,

    /// Deployment ID.
    deployment_id: Uuid,

    metadata: String,

    // rate limiter based on tags
    // NOTE: we assume that there are a finite small number
    // of tags, so using String is fine.
    rate_limiter: TokenBucketRateLimiter<String>,
}

impl ServerState {
    fn new(
        phase: PipelinePhase,
        md: String,
        desired_status: RuntimeDesiredStatus,
        bootstrap_policy: BootstrapPolicy,
        deployment_id: Uuid,
    ) -> Self {
        // Max 10 errors per minute
        let rate_limiter = TokenBucketRateLimiter::new(10, Duration::from_secs(60));
        Self {
            phase: Mutex::new(phase),
            desired_status_change: Arc::default(),
            metadata: md,
            controller: Mutex::new(None),
            checkpoint_state: Default::default(),
            sync_checkpoint_state: Default::default(),
            desired_status: Mutex::new(desired_status),
            bootstrap_policy: Mutex::new(bootstrap_policy),
            deployment_id,
            rate_limiter,
        }
    }

    fn for_error(error: ControllerError, deployment_id: Uuid) -> Self {
        Self::new(
            PipelinePhase::InitializationError(Arc::new(error)),
            String::default(),
            RuntimeDesiredStatus::Paused,
            BootstrapPolicy::Allow,
            deployment_id,
        )
    }

    /// Generate an appropriate error when `state.controller` is set to
    /// `None`, which can mean that the pipeline is initializing, failed to
    /// initialize, has been shut down or failed.
    fn missing_controller_error(&self) -> PipelineError {
        match self.phase() {
            PipelinePhase::Initializing(_) => PipelineError::Initializing,
            PipelinePhase::InitializationError(e) => {
                PipelineError::InitializationError { error: e.clone() }
            }
            PipelinePhase::InitializationComplete => PipelineError::Terminating,
            PipelinePhase::Failed(e) => PipelineError::ControllerError { error: e.clone() },
            PipelinePhase::Suspended => PipelineError::Suspended,
        }
    }

    /// Replaces `e` with a missing controller error if the controller is not set.
    ///
    /// Some endpoints can fail due to a panic that happens while the endpoint was running.
    /// This function checks for this situation  and returns a missing controller error
    /// instead of `e`.
    fn maybe_missing_controller_error(&self, e: ControllerError) -> PipelineError {
        if let Err(missing_controller_error) = self.controller() {
            missing_controller_error
        } else {
            e.into()
        }
    }

    /// Grabs a clone of the controller, or an error if there isn't one.
    fn controller(&self) -> Result<Controller, PipelineError> {
        self.controller
            .lock()
            .unwrap()
            .deref()
            .as_ref()
            .cloned()
            .ok_or_else(|| self.missing_controller_error())
    }

    /// Removes the controller and returns it, or an error if there wasn't one.
    ///
    /// This only makes sense when we're terminating.
    fn take_controller(&self) -> Result<Controller, PipelineError> {
        self.controller
            .lock()
            .unwrap()
            .deref_mut()
            .take()
            .ok_or_else(|| self.missing_controller_error())
    }

    /// Sets the controller.  This should only be done once.
    fn set_controller(&self, controller: Controller) {
        *self.controller.lock().unwrap() = Some(controller);
    }

    fn desired_status(&self) -> RuntimeDesiredStatus {
        *self.desired_status.lock().unwrap()
    }

    pub fn bootstrap_policy(&self) -> BootstrapPolicy {
        *self.bootstrap_policy.lock().unwrap()
    }

    fn set_bootstrap_policy(&self, policy: BootstrapPolicy) {
        *self.bootstrap_policy.lock().unwrap() = policy;
    }

    fn phase(&self) -> PipelinePhase {
        self.phase.lock().unwrap().clone()
    }

    pub fn set_phase(&self, phase: PipelinePhase) {
        *self.phase.lock().unwrap() = phase;
    }
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct ServerArgs {
    /// Pipeline configuration YAML or JSON file.
    ///
    /// We are transitioning from YAML to JSON for configuration.  If
    /// `config_file` ends in `.yaml` then the server will first try to read the
    /// same file with a `.json` extension.
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

    /// Initial runtime desired status.
    #[arg(long)]
    pub initial: RuntimeDesiredStatus,

    /// How to handle bootstrapping when there are changes to the pipeline.
    #[arg(long, default_value_t = BootstrapPolicy::Allow)]
    pub bootstrap_policy: BootstrapPolicy,

    /// UUID generated by the runner for the compute resources that were provisioned to keep this
    /// pipeline process running. It will thus only change if the pipeline is stopped and started
    /// again. If stored in persistent storage, it can be used to determine upon initialization
    /// whether it is a new start (id changed) or an automatic restart (id remains the same).
    #[arg(long)]
    pub deployment_id: Uuid,
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

    let config = parse_config(&args.config_file).inspect_err(|e| {
        // Logging isn't initialized and we can't initialize it until we have
        // the configuration, so just print the message to stderr for now.
        eprintln!("{e}");
    })?;

    let _guard = observability::init("https://f0ec61ff0f8483e9ec8117645ad0c0e1@o4510219052253184.ingest.us.sentry.io/4510299519844352", "pipeline", env!("CARGO_PKG_VERSION"));
    let config_cln = config.clone();
    sentry::configure_scope(|scope| {
        if let Some(id) = config_cln.name.as_ref() {
            scope.set_tag("pipeline.name", id);
        }
    });

    // Initialize the logger by setting its filter and template.
    let pipeline_name = format!(
        "[{}]",
        config
            .given_name
            .as_ref()
            .or(config.name.as_ref())
            .cloned()
            .unwrap_or_default()
    )
    .cyan();
    let pipeline_id = config
        .name
        .as_ref()
        .and_then(|name| name.strip_prefix("pipeline-"))
        .map(|id| id.to_string());
    init_pipeline_logging_with_id(pipeline_name, pipeline_id, get_env_filter(&config))
        .unwrap_or_else(|e| {
            // This happens in unit tests when another test has initialized logging.
            eprintln!("Failed to initialize logging: {e}.")
        });
    if config.global.tracing {
        warn!("Pipeline tracing was enabled but the 'tracing' option was deprecated, use `FELDERA_SENTRY_ENABLED` for tracing.");
    }

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

    /// Does all of the fallible work in creating the controller.
    fn start_controller(
        args: &ServerArgs,
        config: &PipelineConfig,
        circuit_factory: CircuitFactoryFunc,
        runtime: &actix_web::rt::Runtime,
    ) -> Result<WebData<ServerState>, ControllerError> {
        #[cfg(not(feature = "feldera-enterprise"))]
        if config.global.fault_tolerance.is_enabled() {
            return Err(ControllerError::EnterpriseFeature("fault tolerance"));
        }

        // Initiate creating the controller so that we can get access to storage,
        // which is needed to determine the initial state.
        let builder = ControllerBuilder::new(config)?;
        info!(
            "Pipeline deployment identifier from arguments: {}",
            args.deployment_id
        );
        info!("Desired status from arguments: {:?}", args.initial);
        let (initial_status, bootstrap_policy) = match builder
            .storage()
            .and_then(|storage| StoredStatus::read(&*storage))
            .inspect(|stored| {
                info!(
                    "Pipeline deployment identifier from storage: {}",
                    stored.deployment_id
                );
                info!("Desired status from storage: {:?}", stored.desired_status);
            }) {
            Some(stored) if stored.deployment_id == args.deployment_id => {
                // This is an automatic restart (otherwise the deployment ID would
                // have changed).  Use the stored desired status.
                (stored.desired_status, stored.bootstrap_policy)
            }
            Some(stored) => {
                // The pipeline manager restarted us.  Use the desired status it
                // passed in (if it is a valid transition).
                if !stored
                    .desired_status
                    .may_transition_to_at_startup(args.initial)
                {
                    return Err(ControllerError::InvalidStartupTransition {
                        from: stored.desired_status,
                        to: args.initial,
                    });
                }
                (args.initial, args.bootstrap_policy)
            }
            None => {
                // Initial deployment of this pipeline.  Use the desired status
                // passed in by the pipeline manager (it's the only one we have,
                // anyway).
                (args.initial, args.bootstrap_policy)
            }
        };

        let md = match &args.metadata_file {
            None => String::new(),
            Some(metadata_file) => std::fs::read(metadata_file)
                .map_err(|e| {
                    ControllerError::io_error(format!("reading metadata file '{metadata_file}'"), e)
                })
                .and_then(|meta| {
                    String::from_utf8(meta).map_err(|e| {
                        ControllerError::pipeline_config_parse_error(&format!(
                            "invalid UTF8 string in the metadata file '{metadata_file}' ({e})"
                        ))
                    })
                })
                .inspect_err(|error| {
                    error!("{error}");
                })
                .unwrap_or_default(),
        };

        if !matches!(
            initial_status,
            RuntimeDesiredStatus::Running
                | RuntimeDesiredStatus::Paused
                | RuntimeDesiredStatus::Standby,
        ) {
            return Err(ControllerError::InvalidInitialStatus(initial_status));
        }

        let state = WebData::new(ServerState::new(
            PipelinePhase::Initializing(InitializationState::Starting),
            md,
            initial_status,
            bootstrap_policy,
            args.deployment_id,
        ));

        // Initialize the pipeline in a separate thread.  On success, this thread
        // will create a `Controller` instance and store it in `state.controller`.
        let storage = builder.storage().clone();
        thread::Builder::new()
            .name("pipeline-init".to_string())
            .spawn({
                let state = state.clone();
                move || bootstrap(builder, circuit_factory, state)
            })
            .expect("failed to spawn pipeline initialization thread");

        // Spawn a task to update the stored desired state when it changes.
        if let Some(storage) = storage {
            let state = state.clone();
            runtime.spawn(async move {
                let mut prev_stored_status = None;
                loop {
                    let desired_status = state.desired_status();
                    let bootstrap_policy = state.bootstrap_policy();
                    let stored_status = StoredStatus {
                        desired_status,
                        bootstrap_policy,
                        deployment_id: state.deployment_id,
                    };
                    if Some(stored_status) != prev_stored_status {
                        let storage = storage.clone();
                        spawn_blocking(move || stored_status.write(&*storage));
                    }
                    prev_stored_status = Some(stored_status);
                    state.desired_status_change.notified().await;
                }
            });
        }

        Ok(state)
    }

    let system_runner = rt::System::new();

    let state = start_controller(&args, &config, circuit_factory, system_runner.runtime())
        .unwrap_or_else(|error| {
            error!("Initialization failed: {error}");
            WebData::new(ServerState::for_error(error, args.deployment_id))
        });

    let workers = if let Some(http_workers) = config.global.http_workers {
        http_workers as usize
    } else {
        config.global.workers as usize
    };

    let server = HttpServer::new({
        move || {
            let state = state.clone();
            let app = App::new()
                .wrap_fn(|req, srv| {
                    debug!("Request: {} {}", req.method(), req.path());
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
                })
                .wrap(observability::actix_middleware());
            build_app(app, state)
        }
    })
    // The next two settings work around the issue that std::thread::available_parallelism()
    // which is what actix uses internally can't determine the number of threads available
    // in k8s (it will yield the number of cores on the node rather than the number of
    // cores assigned to the cgroup).
    // https://github.com/rust-lang/rust/issues/74479#issuecomment-717097590
    .workers(workers)
    .worker_max_blocking_threads(std::cmp::max(512 / workers, 1))
    .keep_alive(KeepAlive::Timeout(Duration::from_secs(30)))
    .max_connection_rate(1000)
    // The client request timeout sets the time limit for the server to receive
    // the first request from a new client.  When it times out, the server sends
    // a 408 Request Timeout error (and this is the only case where it sends
    // that error).  With the default of 5 seconds, some requests time out in
    // CI.  By extending the timeout, we hope to suppress those problems.  (The
    // timeouts might be a symptom of another problem, though, such as the
    // client in some cases getting stuck and not sending its request.)
    .client_request_timeout(Duration::from_secs(180))
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

    system_runner.block_on(async {
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

fn parse_config(config_file: impl AsRef<Path>) -> Result<PipelineConfig, ControllerError> {
    fn read_config_file_as_string(path: &Path) -> Result<String, ControllerError> {
        let string = std::fs::read(path).map_err(|e| {
            ControllerError::io_error(
                format!("reading configuration file '{}'", path.display()),
                e,
            )
        })?;

        let string = String::from_utf8(string).map_err(|e| {
            ControllerError::pipeline_config_parse_error(&format!(
                "invalid UTF8 string in configuration file '{}' ({e})",
                path.display()
            ))
        })?;

        // Still running without logger here.
        eprintln!("Pipeline configuration read from {}.", path.display());

        Ok(string)
    }

    fn parse_yaml_config(config_file: &Path) -> Result<PipelineConfig, ControllerError> {
        serde_yaml::from_str(&read_config_file_as_string(config_file)?)
            .map_err(|e| ControllerError::pipeline_config_parse_error(&e))
    }

    fn parse_json_config(config_file: &Path) -> Result<PipelineConfig, ControllerError> {
        serde_json::from_str(&read_config_file_as_string(config_file)?)
            .map_err(|e| ControllerError::pipeline_config_parse_error(&e))
    }

    let path = config_file.as_ref();
    let config = if path.extension() == Some(OsStr::new("json")) {
        parse_json_config(path)
    } else {
        let json_path = path.with_extension("json");
        if json_path.exists() {
            parse_json_config(&json_path)
        } else {
            parse_yaml_config(path)
        }
    }?;

    eprintln!(
        "Pipeline configuration loaded successfully: {}",
        config.display_summary()
    );

    Ok(config)
}

// Initialization thread function.
fn bootstrap(
    builder: ControllerBuilder,
    circuit_factory: CircuitFactoryFunc,
    state: WebData<ServerState>,
) {
    do_bootstrap(builder, circuit_factory, &state).unwrap_or_else(|e| {
        // Store error in `state.phase`, so that it can be
        // reported by the server.
        error!("Error initializing the pipeline: {e}.");
        state.set_phase(PipelinePhase::InitializationError(Arc::new(e)));
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
fn error_handler(state: &Weak<ServerState>, error: Arc<ControllerError>, tag: Option<String>) {
    let state = match state.upgrade() {
        None => {
            // if we fail to upgrade state,
            // log the error anyways without checking for rate limiter
            error!("{error}");
            return;
        }
        Some(state) => state,
    };

    // do not rate limit errors without tag
    let should_log = tag.is_none_or(|k| match state.rate_limiter.check(k.clone()) {
        RateLimitCheckResult::Suppressed => false,
        RateLimitCheckResult::Allowed => true,
        RateLimitCheckResult::AllowedAfterSuppression{   suppressed,
                     first_suppression,
                     last_suppression
        }=> {
            let now = state.rate_limiter.now_ms();
            let first_suppressed_elapsed = (now - first_suppression) as f64 / 1000.0;
            let last_suppressed_elapsed = (now - last_suppression) as f64 / 1000.0;
            info!(
                "Suppressed {suppressed} '{k}' errors in the last {first_suppressed_elapsed:.2}s (most recently, {last_suppressed_elapsed:.2}s ago) due to excessive rate."
            );
            true
        }
    });

    if should_log {
        error!("{error}");
    }

    if is_fatal_controller_error(&error) {
        if let Ok(controller) = state.take_controller() {
            state.set_phase(PipelinePhase::Failed(error));
            controller.initiate_stop();
        }
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
    builder: ControllerBuilder,
    circuit_factory: CircuitFactoryFunc,
    state: &WebData<ServerState>,
) -> Result<(), ControllerError> {
    let weak_state_ref = Arc::downgrade(state);
    match state.desired_status() {
        RuntimeDesiredStatus::Unavailable => unreachable!(),
        RuntimeDesiredStatus::Running
        | RuntimeDesiredStatus::Paused
        | RuntimeDesiredStatus::Suspended => {
            // First, if necessary, download the latest checkpoint from S3.
            if let Some(sync) = builder.is_pull_necessary() {
                builder.pull_once(sync)?;
            }
        }
        RuntimeDesiredStatus::Standby => {
            state.set_phase(PipelinePhase::Initializing(InitializationState::Standby));

            builder.continuous_pull(|| state.desired_status() != RuntimeDesiredStatus::Standby)?;

            let mut desired_status = state.desired_status.lock().unwrap();
            if *desired_status == RuntimeDesiredStatus::Standby {
                warn!("Exited standby mode without specifying a new desired state, defaulting to paused");
                *desired_status = RuntimeDesiredStatus::Paused;
                state.desired_status_change.notify_waiters();
            }
        }
    }

    let controller_init = builder.open_checkpoint()?;

    let controller = controller_init.init(
        Some((**state).clone()),
        circuit_factory,
        Box::new(move |e, t| error_handler(&weak_state_ref, e, t))
            as Box<dyn Fn(Arc<ControllerError>, Option<String>) + Send + Sync>,
    )?;

    let desired_status = state.desired_status.lock().unwrap();
    match *desired_status {
        RuntimeDesiredStatus::Unavailable | RuntimeDesiredStatus::Standby => unreachable!(),
        RuntimeDesiredStatus::Paused | RuntimeDesiredStatus::Suspended => controller.pause(),
        RuntimeDesiredStatus::Running => controller.start(),
    };
    state.set_controller(controller);
    drop(desired_status);

    info!("Pipeline initialization complete");
    if let Some(runtime_override) = option_env!("FELDERA_RUNTIME_OVERRIDE") {
        warn!(
            "Pipeline runtime version was overridden and does not match platform version: {}",
            runtime_override
        );
    }
    state.set_phase(PipelinePhase::InitializationComplete);

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
        .service(activate)
        .service(approve)
        .service(status)
        .service(suspendable)
        .service(start_transaction)
        .service(commit_transaction)
        .service(completion_token)
        .service(completion_status)
        .service(query)
        .service(stats)
        .service(error_stats)
        .service(metrics_handler)
        .service(time_series)
        .service(time_series_stream)
        .service(metadata)
        .service(heap_profile)
        .service(dump_profile)
        .service(dump_json_profile)
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

/// Implements `/start`, `/pause`, `/activate`:
///
/// - `action` is the desired action, used in error messages.
///
/// - `new_status` must be [RuntimeDesiredStatus::Paused] or
///   [RuntimeDesiredStatus::Running], as appropriate.
///
/// - `may_activate` is true to allow transitioning out of
///   [RuntimeDesiredStatus::Standby].
async fn state_transition(
    state: WebData<ServerState>,
    action: &'static str,
    new_status: RuntimeDesiredStatus,
    may_activate: bool,
) -> Result<HttpResponse, PipelineError> {
    let mut desired_status = state.desired_status.lock().unwrap();
    let old_status = *desired_status;
    if (may_activate || old_status != RuntimeDesiredStatus::Standby)
        && desired_status.may_transition_to(new_status)
    {
        if new_status != old_status {
            info!("{action}: Transitioning from {old_status:?} to {new_status:?}");
            *desired_status = new_status;
            state.desired_status_change.notify_waiters();
            match state.controller() {
                Ok(controller) => {
                    match (old_status, new_status) {
                        (RuntimeDesiredStatus::Standby, _) => {
                            // `do_bootstrap` will set the new status.
                        }
                        (_, RuntimeDesiredStatus::Paused) => controller.pause(),
                        (_, RuntimeDesiredStatus::Running) => controller.start(),
                        _ => unreachable!(),
                    }
                }
                Err(PipelineError::Initializing) => (),
                Err(error) => return Err(error),
            }
        }
        Ok(HttpResponse::Accepted().json(format!(
            "Pipeline transitioning from {old_status:?} to {new_status:?}"
        )))
    } else {
        Err(PipelineError::InvalidTransition(action, *desired_status))
    }
}

#[get("/start")]
async fn start(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    state_transition(state, "start", RuntimeDesiredStatus::Running, false).await
}

#[get("/pause")]
async fn pause(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    state_transition(state, "pause", RuntimeDesiredStatus::Paused, false).await
}

#[post("/activate")]
async fn activate(
    state: WebData<ServerState>,
    args: Query<ActivateParams>,
) -> Result<HttpResponse, PipelineError> {
    let desired_status = match args.initial.as_str() {
        "paused" => RuntimeDesiredStatus::Paused,
        "running" => RuntimeDesiredStatus::Running,
        _ => {
            return Err(PipelineError::InvalidActivateStatusString(
                args.initial.clone(),
            ))
        }
    };

    match desired_status {
        RuntimeDesiredStatus::Running | RuntimeDesiredStatus::Paused => {
            state_transition(state, "activate", desired_status, true).await
        }
        _ => Err(PipelineError::InvalidActivateStatus(desired_status)),
    }
}

#[post("/approve")]
async fn approve(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    state.set_bootstrap_policy(BootstrapPolicy::Allow);

    // Make sure we don't return until the pipeline has moved on to the next phase
    // (InitializationStatus::Starting). This makes the API synchronous, so the user can
    // expect the pipeline to be out of the AwaitingApproval phase when this method returns.
    while state.phase().is_awaiting_approval() {
        sleep(Duration::from_millis(10)).await;
    }

    Ok(HttpResponse::Ok().json("Bootstrap approved".to_string()))
}

/// Retrieve pipeline runtime status.
///
/// This endpoint is used by the pipeline runner to observe the pipeline in order to determine
/// whether (1) it is healthy, and (2) what it is doing. The endpoint either returns:
///
/// - A `200 OK` with as body [ExtendedRuntimeStatus], containing the current pipeline runtime
///   status and details regarding it. The runner will store this as the observation.
/// - A `503 Service Unavailable` with as body `ErrorResponse`. This indicates a lock cannot be
///   acquired in order to read the runtime status. The runner will observe it as unavailable.
/// - Any other status code with as body `ErrorResponse`. The runner will forcefully stop the
///   pipeline.
///
/// This endpoint is designed to be non-blocking.
#[get("/status")]
async fn status(
    state: WebData<ServerState>,
) -> Result<ExtendedRuntimeStatus, ExtendedRuntimeStatusError> {
    let runtime_desired_status = state.desired_status();
    match state.controller() {
        Ok(controller) => {
            let bootstrapping = controller.status().bootstrap_in_progress();
            return match controller.status().global_metrics.get_state() {
                PipelineState::Paused => Ok(ExtendedRuntimeStatus {
                    runtime_status: if bootstrapping {
                        RuntimeStatus::Bootstrapping
                    } else {
                        RuntimeStatus::Paused
                    },
                    runtime_status_details: json!(""),
                    runtime_desired_status,
                }),
                PipelineState::Running => Ok(ExtendedRuntimeStatus {
                    runtime_status: if bootstrapping {
                        RuntimeStatus::Bootstrapping
                    } else {
                        RuntimeStatus::Running
                    },
                    runtime_status_details: json!(""),
                    runtime_desired_status,
                }),
                PipelineState::Terminated => Err(ExtendedRuntimeStatusError {
                    status_code: StatusCode::INTERNAL_SERVER_ERROR,
                    error: feldera_types::error::ErrorResponse {
                        message: "Pipeline has been terminated.".to_string(),
                        error_code: Cow::from("PipelineTerminated"),
                        details: json!({}),
                    },
                }),
            };
        }
        Err(_) => {
            // Controller isn't set.
        }
    };

    // The controller is not set: acquire the phase read lock
    match state.phase() {
        PipelinePhase::Initializing(inner) => match inner {
            InitializationState::Starting => Ok(ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Initializing,
                runtime_status_details: json!(""),
                runtime_desired_status,
            }),
            InitializationState::DownloadingCheckpoint => Ok(ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Initializing,
                runtime_status_details: json!("downloading checkpoint from object storage"),
                runtime_desired_status,
            }),
            InitializationState::Standby => Ok(ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Standby,
                runtime_status_details: json!(""),
                runtime_desired_status,
            }),
            InitializationState::AwaitingApproval(diff) => Ok(ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::AwaitingApproval,
                runtime_status_details: serde_json::to_value(&diff).unwrap_or_default(),
                runtime_desired_status,
            }),
        },
        PipelinePhase::InitializationError(e) => {
            let e = PipelineError::InitializationError { error: e.clone() };
            let status_code = e.status_code();
            let e = ErrorResponse::from_error(&e);
            Err(ExtendedRuntimeStatusError {
                status_code,
                error: feldera_types::error::ErrorResponse {
                    message: e.message,
                    error_code: e.error_code,
                    details: e.details,
                },
            })
        }
        PipelinePhase::InitializationComplete => Err(ExtendedRuntimeStatusError {
            status_code: StatusCode::INTERNAL_SERVER_ERROR,
            error: feldera_types::error::ErrorResponse {
                message: "Pipeline initialization was completed but the controller was not set."
                    .to_string(),
                error_code: Cow::from("ControllerMissingAfterInitialization"),
                details: json!({}),
            },
        }),
        PipelinePhase::Failed(e) => {
            let e = e.clone();
            if matches!(*e, ControllerError::RestoreInProgress) {
                Ok(ExtendedRuntimeStatus {
                    runtime_status: RuntimeStatus::Replaying,
                    runtime_status_details: json!(""),
                    runtime_desired_status,
                })
            } else {
                let mut status_code = e.status_code();
                if status_code == StatusCode::SERVICE_UNAVAILABLE {
                    error!("Endpoint /status: unexpected status code {status_code} has been converted into {} for error {e}", StatusCode::SERVICE_UNAVAILABLE);
                    status_code = StatusCode::INTERNAL_SERVER_ERROR;
                }
                let e = PipelineError::ControllerError { error: e };
                let e = ErrorResponse::from_error(&e);
                Err(ExtendedRuntimeStatusError {
                    status_code,
                    error: feldera_types::error::ErrorResponse {
                        message: e.message,
                        error_code: e.error_code,
                        details: e.details,
                    },
                })
            }
        }
        PipelinePhase::Suspended => Ok(ExtendedRuntimeStatus {
            runtime_status: RuntimeStatus::Suspended,
            runtime_status_details: json!(""),
            runtime_desired_status,
        }),
    }
}

/// Retrieve whether a pipeline is suspendable or not.
#[get("/suspendable")]
async fn suspendable(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    let suspend_error = state
        .controller()?
        .status()
        .suspend_error
        .lock()
        .unwrap()
        .clone();
    let reasons = match suspend_error {
        Some(SuspendError::Permanent(errors)) => Some(errors.clone()),
        _ => None,
    };
    Ok(HttpResponse::Ok().json(SuspendableResponse::new(
        reasons.is_none(),
        reasons.unwrap_or_default(),
    )))
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
    let args = args.into_inner();
    tracing::debug!("processing adhoc query: {:?}", args.sql);
    let session_ctxt = state.controller()?.session_context()?;
    if !request_is_websocket(&request) {
        stream_adhoc_result(args, session_ctxt).await
    } else {
        adhoc_websocket(session_ctxt, request, stream).await
    }
}

#[get("/stats")]
async fn stats(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    Ok(HttpResponse::Ok().json(state.controller()?.status()))
}

/// This endpoint returns a subset of stats that don't need updating and so is more performant than /stats
#[get("/stats/errors")]
async fn error_stats(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    let controller = state.controller()?;
    let controller_status = controller.stale_status();

    let inputs: Vec<EndpointErrorStats<InputEndpointErrorMetrics>> = controller_status
        .input_status()
        .values()
        .map(|input| EndpointErrorStats {
            metrics: InputEndpointErrorMetrics {
                endpoint_name: input.endpoint_name.clone(),
                num_transport_errors: input.metrics.num_transport_errors.load(Ordering::Acquire),
                num_parse_errors: input.metrics.num_parse_errors.load(Ordering::Acquire),
            },
        })
        .collect();

    let outputs: Vec<EndpointErrorStats<OutputEndpointErrorMetrics>> = controller_status
        .output_status()
        .values()
        .map(|output| EndpointErrorStats {
            metrics: OutputEndpointErrorMetrics {
                endpoint_name: output.endpoint_name.clone(),
                num_transport_errors: output.metrics.num_transport_errors.load(Ordering::Acquire),
                num_encode_errors: output.metrics.num_encode_errors.load(Ordering::Acquire),
            },
        })
        .collect();

    Ok(HttpResponse::Ok().json(PipelineStatsErrorsResponse { inputs, outputs }))
}

/// Retrieve circuit metrics.
#[get("/metrics")]
async fn metrics_handler(
    state: WebData<ServerState>,
    query_params: web::Query<MetricsParameters>,
) -> Result<HttpResponse, PipelineError> {
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
        let labels = if let Some(given_name) = &controller_status.pipeline_config.given_name {
            labels.with("pipeline_name", given_name)
        } else {
            labels
        };
        controller.write_metrics(&mut metrics_writer, &labels, controller_status);
        metrics_writer.into_output()
    }

    let controller = state.controller()?;
    match &query_params.format {
        MetricsFormat::Prometheus => Ok(HttpResponse::Ok()
            .content_type(mime::TEXT_PLAIN)
            .body(serialize_metrics::<PrometheusFormatter>(&controller))),
        MetricsFormat::Json => Ok(HttpResponse::Ok()
            .content_type(mime::APPLICATION_JSON)
            .body(serialize_metrics::<JsonFormatter>(&controller))),
    }
}

/// Retrieve time series for basic statistics.
#[get("/time_series")]
async fn time_series(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    let time_series = TimeSeries {
        now: Utc::now(),
        samples: state
            .controller()?
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

/// Stream time series for basic statistics.
///
/// Returns a snapshot of all existing time series data followed by a stream of
/// new time series data points as they become available. Each line in the response
/// is a JSON object representing a single time series data point.
#[get("/time_series_stream")]
async fn time_series_stream(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    let controller = state.controller()?;
    let controller_status = controller.status();

    // Get existing time series data as snapshot
    let existing_samples: Vec<_> = controller_status
        .time_series
        .lock()
        .unwrap()
        .iter()
        .cloned()
        .collect();

    // Subscribe to future updates
    let receiver = controller_status.time_series_notifier.subscribe();
    let stream = BroadcastStream::new(receiver);

    let response_stream = async_stream::stream! {
        // First, yield all existing samples
        for sample in existing_samples {
            let line = format!("{}\n", serde_json::to_string(&sample).unwrap());
            yield Ok::<_, actix_web::Error>(web::Bytes::from(line));
        }

        // Then yield new samples as they arrive
        let mut stream = stream;
        while let Some(result) = stream.next().await {
            if let Ok(sample) = result {
                let line = format!("{}\n", serde_json::to_string(&sample).unwrap());
                yield Ok::<_, actix_web::Error>(web::Bytes::from(line));
            }
        }
    };

    Ok(HttpResponse::Ok()
        .content_type("application/x-ndjson")
        .streaming(response_stream))
}

/// Returns static metadata associated with the circuit.  Can contain any
/// string, but the intention is to store information about the circuit (e.g.,
/// SQL code) in JSON format.
#[get("/metadata")]
async fn metadata(state: WebData<ServerState>) -> impl Responder {
    HttpResponse::Ok()
        .content_type(mime::APPLICATION_JSON)
        .body(state.metadata.clone())
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
                .insert_header(header::ContentEncoding::Identity)
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
async fn dump_profile(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    Ok(HttpResponse::Ok()
        .insert_header(header::ContentType("application/zip".parse().unwrap()))
        .insert_header(header::ContentDisposition::attachment("profile.zip"))
        .insert_header(header::ContentEncoding::Identity)
        .body(state.controller()?.async_graph_profile().await?.as_zip()))
}

#[get("/dump_json_profile")]
async fn dump_json_profile(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    Ok(HttpResponse::Ok()
        .insert_header(header::ContentType("application/json".parse().unwrap()))
        .insert_header(header::ContentDisposition::attachment("profile.json"))
        .body(state.controller()?.async_json_profile().await?.as_json()))
}

/// Dump the low-level IR of the circuit.
#[get("/lir")]
async fn lir(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    Ok(HttpResponse::Ok()
        .insert_header(header::ContentType("application/zip".parse().unwrap()))
        .insert_header(header::ContentDisposition::attachment("lir.zip"))
        .body(state.controller()?.lir().as_zip()))
}

#[post("/checkpoint/sync")]
async fn checkpoint_sync(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    let controller = state.controller()?;

    let Some(last_checkpoint) = state.checkpoint_state.lock().unwrap().last_checkpoint else {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
                    message: "no checkpoints found; make a POST request to `/checkpoint` to make a new checkpoint".to_string(),
                    error_code: "400".into(),
                    details: serde_json::Value::Null,
                }));
    };

    spawn(async move {
        let result = controller.async_sync_checkpoint(last_checkpoint).await;
        state
            .sync_checkpoint_state
            .lock()
            .unwrap()
            .completed(last_checkpoint, result);
    });

    Ok(HttpResponse::Accepted().json(CheckpointSyncResponse::new(last_checkpoint)))
}

/// Initiates a checkpoint and returns its sequence number.  The caller may poll
/// `/checkpoint_status` to determine when the checkpoint completes.
#[post("/checkpoint")]
async fn checkpoint(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    let controller = state.controller()?;
    let seq = state.checkpoint_state.lock().unwrap().next_seq();
    spawn(async move {
        let result = controller.async_checkpoint().await;
        state
            .checkpoint_state
            .lock()
            .unwrap()
            .completed(seq, result.map(|c| c.circuit));
    });
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

/// Suspends the pipeline and terminate the circuit.
///
/// This implementation is designed to be idempotent, so that any number of
/// suspend requests act like just one.
#[post("/suspend")]
async fn suspend(state: WebData<ServerState>) -> Result<impl Responder, PipelineError> {
    let mut desired_status = state.desired_status.lock().unwrap();
    match *desired_status {
        RuntimeDesiredStatus::Unavailable => unreachable!(),
        RuntimeDesiredStatus::Standby => {
            return Err(PipelineError::InvalidTransition("suspend", *desired_status))
        }
        RuntimeDesiredStatus::Running | RuntimeDesiredStatus::Paused => {
            info!("suspend: Transitioning from {desired_status:?} to Suspended");
            *desired_status = RuntimeDesiredStatus::Suspended;
            state.desired_status_change.notify_waiters();
            drop(desired_status);

            async fn suspend(state: WebData<ServerState>) {
                loop {
                    if let Ok(controller) = state.controller() {
                        if let Err(error) = controller.async_suspend().await {
                            error!("controller suspend failed ({error})");
                        }
                        break;
                    }

                    match state.phase() {
                        PipelinePhase::Initializing(_) | PipelinePhase::InitializationComplete => {}
                        PipelinePhase::InitializationError(_)
                        | PipelinePhase::Failed(_)
                        | PipelinePhase::Suspended => break,
                    };
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                state.set_phase(PipelinePhase::Suspended);
                if let Ok(controller) = state.take_controller() {
                    if let Err(error) = controller.async_stop().await {
                        error!("stopping controller failed ({error})");
                    }
                }
            }

            // This can only be spawned once, because we only transition to
            // [RuntimeDesiredStatus::Suspended] once and we never transition
            // out of it.
            tokio::spawn(async move { suspend(state).await });
        }
        RuntimeDesiredStatus::Suspended => (),
    };
    Ok(HttpResponse::Accepted().json("Pipeline is suspending"))
}

#[post("/start_transaction")]
async fn start_transaction(state: WebData<ServerState>) -> Result<impl Responder, PipelineError> {
    Ok(HttpResponse::Ok().json(state.controller()?.start_transaction()?))
}

#[post("/commit_transaction")]
async fn commit_transaction(state: WebData<ServerState>) -> Result<impl Responder, PipelineError> {
    state.controller()?.start_commit_transaction()?;
    Ok(HttpResponse::Ok().json("Transaction commit initiated"))
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
    let controller = state.controller()?;
    if controller.register_api_connection().is_err() {
        return Err(PipelineError::ApiConnectionLimit);
    }

    controller
        .add_input_endpoint(
            &endpoint_name,
            config,
            Box::new(endpoint.clone()) as Box<dyn TransportInputEndpoint>,
            None,
        )
        .inspect_err(|e| {
            controller.unregister_api_connection();
            debug!("Failed to create API endpoint: '{e}'");
        })?;

    Ok(endpoint)
}

#[post("/ingress/{table_name}")]
async fn input_endpoint(
    state: WebData<ServerState>,
    path: web::Path<String>,
    req: HttpRequest,
    args: Query<IngressArgs>,
    payload: Payload,
) -> Result<HttpResponse, PipelineError> {
    thread_local! {
        static TABLE_ENDPOINTS: RefCell<HashMap<(String, FormatConfig), HttpInputEndpoint, BuildHasherDefault<DefaultHasher>>> = const {
            RefCell::new(HashMap::with_hasher(BuildHasherDefault::new()))
        };
    }
    debug!("{req:?}");

    let table_name = path.into_inner();

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

    let token = state
        .controller()?
        .completion_token(endpoint.name())?
        .encode();
    Ok(HttpResponse::Ok().json(CompletionTokenResponse::new(token)))
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
        config: serde_json::to_value(config)
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
        config: serde_json::to_value(config)
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
    path: web::Path<String>,
    req: HttpRequest,
    args: Query<EgressArgs>,
) -> Result<HttpResponse, PipelineError> {
    debug!("/egress request:{req:?}");

    let table_name = path.into_inner();

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
    let controller = state.controller()?;
    if controller.register_api_connection().is_err() {
        return Err(PipelineError::ApiConnectionLimit);
    }

    let endpoint_id = controller
        .add_output_endpoint(
            &endpoint_name,
            &config,
            Box::new(endpoint.clone()) as Box<dyn OutputEndpoint>,
            None,
        )
        .inspect_err(|_| {
            controller.unregister_api_connection();
        })?;

    // We need to pass a callback to `request` to disconnect the endpoint when the
    // request completes.  Use a downgraded reference to `state`, so
    // this closure doesn't prevent the controller from shutting down.
    let weak_state = Arc::downgrade(&state);

    // Call endpoint to create a response with a streaming body, which will be
    // evaluated after we return the response object to actix.
    Ok(endpoint.request(Box::new(move || {
        // Delete endpoint on completion/error.
        // We don't control the lifetime of the response object after
        // returning it to actix, so the only way to run cleanup code
        // when the HTTP request terminates is to piggyback on the
        // destructor.
        if let Some(state) = weak_state.upgrade() {
            if let Ok(controller) = state.controller() {
                controller.disconnect_output(&endpoint_id);
                controller.unregister_api_connection();
            }
        }
    })))
}

/// This service journals the paused state, but it does not wait for the journal
/// record to commit before it returns success, so there is a small race.
#[get("/input_endpoints/{endpoint_name}/pause")]
async fn pause_input_endpoint(
    state: WebData<ServerState>,
    path: web::Path<String>,
) -> Result<HttpResponse, PipelineError> {
    state.controller()?.pause_input_endpoint(&path)?;
    Ok(HttpResponse::Ok().into())
}

#[get("/input_endpoints/{endpoint_name}/stats")]
async fn input_endpoint_status(
    state: WebData<ServerState>,
    path: web::Path<String>,
) -> Result<HttpResponse, PipelineError> {
    let ep_stats = state.controller()?.input_endpoint_status(&path)?;
    Ok(HttpResponse::Ok().json(ep_stats))
}

#[get("/output_endpoints/{endpoint_name}/stats")]
async fn output_endpoint_status(
    state: WebData<ServerState>,
    path: web::Path<String>,
) -> Result<HttpResponse, PipelineError> {
    Ok(HttpResponse::Ok().json(state.controller()?.output_endpoint_status(&path)?))
}

/// This service journals the paused state, but it does not wait for the journal
/// record to commit before it returns success, so there is a small race.
#[get("/input_endpoints/{endpoint_name}/start")]
async fn start_input_endpoint(
    state: WebData<ServerState>,
    path: web::Path<String>,
) -> Result<HttpResponse, PipelineError> {
    state.controller()?.start_input_endpoint(&path)?;
    Ok(HttpResponse::Ok().into())
}

/// Generate a completion token for the endpoint.
#[get("/input_endpoints/{endpoint_name}/completion_token")]
async fn completion_token(
    state: WebData<ServerState>,
    path: web::Path<String>,
) -> Result<HttpResponse, PipelineError> {
    Ok(HttpResponse::Ok().json(CompletionTokenResponse::new(
        state.controller()?.completion_token(&path)?.encode(),
    )))
}

/// Check the status of a completion token.
#[get("/completion_status")]
async fn completion_status(
    state: WebData<ServerState>,
    args: Query<CompletionStatusArgs>,
) -> Result<HttpResponse, PipelineError> {
    let token = CompletionToken::decode(&args.token).map_err(|e| PipelineError::InvalidParam {
        error: format!("invalid completion token: {e}"),
    })?;
    if state
        .controller()?
        .completion_status(&token)
        .map_err(|e| state.maybe_missing_controller_error(e))?
    {
        Ok(HttpResponse::Ok().json(CompletionStatusResponse::complete()))
    } else {
        Ok(HttpResponse::Accepted().json(CompletionStatusResponse::inprogress()))
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct StoredStatus {
    /// Desired status.
    desired_status: RuntimeDesiredStatus,

    bootstrap_policy: BootstrapPolicy,

    // Deployment ID.
    deployment_id: Uuid,
}

impl StoredStatus {
    fn read(storage: &dyn StorageBackend) -> Option<Self> {
        match storage.read_json::<StoredStatus>(&StoragePath::from(STATUS_FILE)) {
            Ok(s) => Some(s),
            Err(e) if e.kind() == ErrorKind::NotFound => None,
            Err(e) => {
                warn!("{STATUS_FILE}: failed to read from storage ({e})");
                None
            }
        }
    }

    fn write(&self, storage: &dyn StorageBackend) {
        if let Err(e) = storage
            .write_json(&StoragePath::from(STATUS_FILE), self)
            .and_then(|reader| reader.commit())
        {
            warn!("{STATUS_FILE}: failed to write to storage ({e})");
        }
    }
}

#[cfg(test)]
#[cfg(feature = "with-kafka")]
mod test_with_kafka {
    use super::{bootstrap, build_app, parse_config, ServerArgs, ServerState};
    use crate::{
        controller::{ControllerBuilder, MAX_API_CONNECTIONS},
        ensure_default_crypto_provider,
        server::{InitializationState, PipelinePhase},
        test::{
            async_wait, generate_test_batches,
            http::{TestHttpReceiver, TestHttpSender},
            kafka::{BufferConsumer, KafkaResources, TestProducer},
            test_circuit, TestStruct,
        },
    };
    use actix_test::TestServer;
    use actix_web::{http::StatusCode, middleware::Logger, web::Data as WebData, App};
    use feldera_types::runtime_status::RuntimeDesiredStatus;
    use feldera_types::{
        completion_token::{CompletionStatus, CompletionStatusResponse, CompletionTokenResponse},
        runtime_status::BootstrapPolicy,
    };
    use proptest::{
        strategy::{Strategy, ValueTree},
        test_runner::TestRunner,
    };
    use serde_json::{self, json, Value as JsonValue};
    use std::{
        io::Write,
        thread,
        thread::sleep,
        time::{Duration, Instant},
    };
    use tempfile::NamedTempFile;
    use uuid::Uuid;

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
        let buffer_consumer =
            BufferConsumer::new("test_server_output_topic", "csv", json!({}), None);

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

        let state = WebData::new(ServerState::new(
            PipelinePhase::Initializing(InitializationState::Starting),
            String::new(),
            RuntimeDesiredStatus::Paused,
            BootstrapPolicy::Allow,
            Uuid::new_v4(),
        ));
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
            initial: RuntimeDesiredStatus::Paused,
            bootstrap_policy: BootstrapPolicy::Allow,
            deployment_id: uuid::Uuid::nil(),
        };

        let config = parse_config(&args.config_file).unwrap();
        let builder = ControllerBuilder::new(&config).unwrap();
        thread::spawn(move || {
            bootstrap(
                builder,
                Box::new(|workers| {
                    Ok(test_circuit::<TestStruct>(
                        workers,
                        &TestStruct::schema(),
                        &[None],
                    ))
                }),
                state_clone,
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

        drop(buffer_consumer);
        drop(kafka_resources);
    }

    #[actix_web::test]
    async fn test_transactions() {
        ensure_default_crypto_provider();

        // We cannot use proptest macros in `async` context, so generate
        // some random data manually.
        let mut runner = TestRunner::default();
        let data = generate_test_batches(0, 100, 1000)
            .new_tree(&mut runner)
            .unwrap()
            .current();

        // Config string
        let config_str = r#"
name: test
inputs:
outputs:
"#;

        let mut config_file = NamedTempFile::new().unwrap();
        config_file.write_all(config_str.as_bytes()).unwrap();

        println!("Creating HTTP server");

        let state = WebData::new(ServerState::new(
            PipelinePhase::Initializing(InitializationState::Starting),
            String::default(),
            RuntimeDesiredStatus::Paused,
            BootstrapPolicy::Allow,
            Uuid::default(),
        ));
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
            initial: RuntimeDesiredStatus::Paused,
            bootstrap_policy: BootstrapPolicy::Allow,
            deployment_id: Uuid::default(),
        };

        let config = parse_config(&args.config_file).unwrap();
        let builder = ControllerBuilder::new(&config).unwrap();
        thread::spawn(move || {
            bootstrap(
                builder,
                Box::new(|workers| {
                    Ok(test_circuit::<TestStruct>(
                        workers,
                        &TestStruct::schema(),
                        &[None],
                    ))
                }),
                state_clone,
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

        // Start pipeline.
        println!("/start");
        let resp = server.get("/start").send().await.unwrap();
        assert!(resp.status().is_success());

        println!("/start_transaction");
        let resp = server.post("/start_transaction").send().await.unwrap();
        assert!(resp.status().is_success());

        println!("Connecting to HTTP output endpoint");
        let mut egress_resp = server
            .post("/egress/test_output1?backpressure=true")
            .send()
            .await
            .unwrap();

        let req = server.post("/ingress/test_input1");

        TestHttpSender::send_stream(req, &data).await;
        println!("data sent");

        println!("/commit_transaction");
        let resp = server.post("/commit_transaction").send().await.unwrap();
        assert!(resp.status().is_success());

        TestHttpReceiver::wait_for_output_unordered(&mut egress_resp, &data).await;
    }
}
