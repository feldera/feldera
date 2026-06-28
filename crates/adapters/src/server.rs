use crate::adhoc::set_snapshot;
use crate::controller::{CompletionToken, ConsistentSnapshot, ControllerBuilder};
use crate::format::csv::CsvOutputFormat;
use crate::format::get_input_format;
use crate::format::json::JsonOutputFormat;
use crate::panic::enable_counting_panics;
use crate::server::metrics::{
    JsonFormatter, LabelStack, MetricsFormatter, MetricsWriter, PrometheusFormatter,
};
use crate::static_compile::catalog::OUTPUT_MAPPING;
use crate::transport::http::HttpOutputFormat;
use crate::util::{LongOperationWarning, RateLimitCheckResult, TokenBucketRateLimiter};
use crate::{Catalog, dyn_event};
use crate::{
    CircuitCatalog, Controller, ControllerError, FormatConfig, InputEndpointConfig, OutputEndpoint,
    OutputEndpointConfig, PipelineConfig, TransportInputEndpoint,
    adhoc::{adhoc_websocket, stream_adhoc_result},
    controller::ConnectorConfig,
    ensure_default_crypto_provider,
    samply::{SamplyProfile, SamplyState, SamplyStatus},
    transport::http::{
        HttpInputEndpoint, HttpInputTransport, HttpOutputEndpoint, HttpOutputTransport,
    },
};
use actix_web::HttpResponseBuilder;
use actix_web::body::MessageBody;
use actix_web::dev::Service;
use actix_web::http::KeepAlive;
use actix_web::{
    App, Error as ActixError, HttpRequest, HttpResponse, HttpServer, Responder, ResponseError,
    dev::{ServiceFactory, ServiceRequest},
    get,
    http::StatusCode,
    http::header,
    post, rt,
    web::{self, Data as WebData, Payload, Query},
};
use arrow::ipc::writer::StreamWriter;
use async_stream;
use bytes::Bytes;
use chrono::Utc;
use clap::Parser;
use colored::Colorize;
use dbsp::circuit::Layout;
use dbsp::circuit::checkpointer::Checkpointer;
use dbsp::{DBSPHandle, circuit::CircuitConfig};
use dbsp::{RootCircuit, Runtime};
use dyn_clone::DynClone;
use feldera_adapterlib::PipelineState;
use feldera_adapterlib::errors::controller::ConfigError;
use feldera_observability as observability;
use feldera_observability::json_logging::init_pipeline_logging_with_id;
use feldera_storage::{StorageBackend, StoragePath};
use feldera_types::adapter_stats::{
    EndpointErrorStats, InputEndpointErrorMetrics, OutputEndpointErrorMetrics,
    PipelineStatsErrorsResponse,
};
use feldera_types::checkpoint::{
    CheckpointFailure, CheckpointPullStatus, CheckpointResponse, CheckpointStatus,
    CheckpointSyncFailure, CheckpointSyncResponse, CheckpointSyncStatus, HostInfo,
};
use feldera_types::completion_token::{
    CompletionStatusArgs, CompletionStatusResponse, CompletionTokenResponse,
};
use feldera_types::config::SyncConfig;
use feldera_types::constants::STATUS_FILE;
use feldera_types::coordination::{
    AdHocScan, CoordinationActivate, CoordinationStatus, Labels, RestartArgs, Step, StepRequest,
};
use feldera_types::format::json::JsonEncoderConfig;
use feldera_types::pipeline_diff::PipelineDiff;
use feldera_types::query_params::{
    ActivateParams, ApproveParameters, MetricsFormat, MetricsParameters, SamplyProfileGetParams,
    SamplyProfileParams,
};
use feldera_types::runtime_status::{
    BootstrapConfig, BootstrapPolicy, ExtendedRuntimeStatus, ExtendedRuntimeStatusError,
    RuntimeDesiredStatus, RuntimeStatus, StorageStatusDetails,
};
use feldera_types::suspend::{SuspendError, SuspendableResponse};
use feldera_types::time_series::TimeSeries;
use feldera_types::transport::http::HttpOutputConfig;
use feldera_types::{
    checkpoint::CheckpointMetadata, config::TransportConfig, transport::http::HttpInputConfig,
};
use feldera_types::{query::AdhocQueryArgs, transport::http::SERVER_PORT_FILE};
use futures::StreamExt;
use futures::stream::unfold;
use futures_util::FutureExt;
use futures_util::stream::once;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::ffi::OsStr;
use std::hash::{BuildHasherDefault, DefaultHasher, Hash, Hasher};
use std::io::ErrorKind;
use std::mem::take;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::{
    borrow::Cow,
    net::TcpListener,
    sync::{Arc, Mutex, Weak},
    thread,
};
use tokio::spawn;
use tokio::sync::Notify;
use tokio::task::spawn_blocking;
use tokio::time::{sleep, timeout};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::WatchStream;
use tracing::{Instrument, Level, debug, error, info, info_span, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

pub mod error;
pub mod metrics;

#[cfg(target_family = "unix")]
mod stack_overflow_backtrace;

pub use self::error::{ErrorResponse, MAX_REPORTED_PARSE_ERRORS, PipelineError};

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

    fn completed_periodic(&mut self, uuid: uuid::Uuid) {
        self.status.periodic = Some(uuid);
    }
}

#[derive(Clone, Debug, Default)]
struct CheckpointState {
    /// Sequence number for the next checkpoint request.
    next_seq: u64,

    /// The UUID of the last checkpoint.
    last_checkpoint: Option<uuid::Uuid>,

    /// Status to report to user (success/failure only; `activity` is computed
    /// live from the watch channel).
    status: CheckpointStatus,
}

impl CheckpointState {
    /// Returns the sequence number to use for the next checkpoint request and
    /// records it as the in-flight checkpoint.
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
                        failed_at: Utc::now(),
                    });
                }
            }
        }
    }
}

pub(crate) struct ServerState {
    /// The other locks in this structure nest inside `desired_status`.
    desired_status: Mutex<RuntimeDesiredStatus>,

    /// Bootstrap configuration.
    bootstrap_config: Mutex<BootstrapConfig>,

    /// Notified when `desired_status` or `phase` changes.
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

    /// Leaf lock.
    /// Samply profiling state.
    samply_state: Arc<Mutex<SamplyState>>,

    /// Deployment ID.
    ///
    /// This comes from [ServerArgs].  It remains unchanged across automatic
    /// restarts.
    deployment_id: Uuid,

    /// Incarnation UUID.
    ///
    /// This is randomly generated each time the process starts.
    incarnation_uuid: Uuid,

    metadata: String,

    storage: Option<Arc<dyn StorageBackend>>,

    /// Sync configuration, extracted from the storage backend at startup.
    ///
    /// Used by the `/coordination/checkpoint/pull` endpoint to pull checkpoints
    /// from object storage on behalf of the multihost coordinator.
    sync_config: Option<SyncConfig>,

    /// Host identity of this pod within a multihost pipeline, derived from
    /// `--host-id` and `config.global.hosts` at startup.  `None` for solo
    /// pipelines.
    host_info: Option<HostInfo>,

    /// Status of the most recent background checkpoint pull.
    pull_state: Mutex<CheckpointPullStatus>,

    // rate limiter based on tags
    // NOTE: we assume that there are a finite small number
    // of tags, so using String is fine.
    rate_limiter: TokenBucketRateLimiter<String>,

    coordination_activate: Mutex<Option<CoordinationActivate>>,

    /// Current collection of leases.
    leases: Mutex<HashMap<Step, Lease>>,
}

/// Leases on snapshots of particular steps.
///
/// A lease gives the coordinator the ability to read tables within a step with
/// `/coordination/adhoc/scan`, to enable multihost ad-hoc queries.
struct Lease {
    /// Number of leaseholders (usually 1).
    reference_count: usize,

    /// The snapshot held by the lease.
    snapshot: ConsistentSnapshot,
}

impl ServerState {
    #[allow(clippy::too_many_arguments)]
    fn new(
        phase: PipelinePhase,
        md: String,
        desired_status: RuntimeDesiredStatus,
        bootstrap_config: BootstrapConfig,
        deployment_id: Uuid,
        storage: Option<Arc<dyn StorageBackend>>,
        sync_config: Option<SyncConfig>,
        host_info: Option<HostInfo>,
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
            bootstrap_config: Mutex::new(bootstrap_config),
            deployment_id,
            storage,
            sync_config,
            host_info,
            pull_state: Default::default(),
            rate_limiter,
            samply_state: Default::default(),
            coordination_activate: Default::default(),
            leases: Default::default(),
            incarnation_uuid: Uuid::now_v7(),
        }
    }

    fn for_error(error: ControllerError, deployment_id: Uuid) -> Self {
        Self::new(
            PipelinePhase::InitializationError(Arc::new(error)),
            String::default(),
            RuntimeDesiredStatus::Paused,
            BootstrapConfig::default(),
            deployment_id,
            None,
            None,
            None,
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
        match self.controller() {
            Err(missing_controller_error) => missing_controller_error,
            _ => e.into(),
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

    pub fn bootstrap_config(&self) -> BootstrapConfig {
        *self.bootstrap_config.lock().unwrap()
    }

    fn set_bootstrap_config(&self, config: BootstrapConfig) {
        *self.bootstrap_config.lock().unwrap() = config;
    }

    fn phase(&self) -> PipelinePhase {
        self.phase.lock().unwrap().clone()
    }

    pub fn set_phase(&self, phase: PipelinePhase) {
        *self.phase.lock().unwrap() = phase;
        self.desired_status_change.notify_waiters();
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

    /// Bootstrap the pipeline with output connectors disabled.
    #[arg(long, action = clap::ArgAction::SetTrue)]
    pub silent_bootstrap: bool,

    /// UUID generated by the runner for the compute resources that were provisioned to keep this
    /// pipeline process running. It will thus only change if the pipeline is stopped and started
    /// again. If stored in persistent storage, it can be used to determine upon initialization
    /// whether it is a new start (id changed) or an automatic restart (id remains the same).
    #[arg(long)]
    pub deployment_id: Uuid,

    /// Host identifier for this pipeline instance in multihost deployments.
    /// Helps distinguish logs originating from different hosts.
    #[arg(long)]
    pub host_id: Option<usize>,
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
    enable_counting_panics();

    let bind_address = args.bind_address.clone();
    let port = args.default_port.unwrap_or(0);
    let listener = TcpListener::bind((bind_address, port))
        .map_err(|e| ControllerError::io_error(format!("binding to TCP port {port}"), e))?;

    let port = listener
        .local_addr()
        .map_err(|e| {
            ControllerError::io_error("retrieving local socket address of the TCP listener", e)
        })?
        .port();

    let config = parse_config(&args.config_file).inspect_err(|e| {
        // Logging isn't initialized and we can't initialize it until we have
        // the configuration, so just print the message to stderr for now.
        eprintln!("{e}");
    })?;

    let _guard = observability::init(
        "https://f0ec61ff0f8483e9ec8117645ad0c0e1@o4510219052253184.ingest.us.sentry.io/4510299519844352",
        "pipeline",
        env!("CARGO_PKG_VERSION"),
    );
    let config_cln = config.clone();
    sentry::configure_scope(|scope| {
        if let Some(id) = config_cln.name.as_ref() {
            scope.set_tag("pipeline.name", id);
        }
    });

    // Initialize the logger by setting its filter and template.
    let pipeline_label = config
        .given_name
        .as_ref()
        .or(config.name.as_ref())
        .cloned()
        .unwrap_or_default();
    let pipeline_name = if let Some(host_id) = args.host_id.as_ref() {
        format!("[{} host={}]", pipeline_label, host_id).cyan()
    } else {
        format!("[{}]", pipeline_label).cyan()
    };

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
    if let Some(provider) = rustls::crypto::CryptoProvider::get_default() {
        observability::fips::log_rustls_provider_fips_status(
            "startup default provider",
            provider.fips(),
        );
    }
    if config.global.tracing {
        warn!(
            "Pipeline tracing was enabled but the 'tracing' option was deprecated, use `FELDERA_SENTRY_ENABLED` for tracing."
        );
    }

    // Install stack overflow handler early, before creating the controller and parsing DevTweaks.
    #[cfg(target_family = "unix")]
    if config.global.dev_tweaks.stack_overflow_backtrace() {
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
        let (initial_status, bootstrap_config) = match builder
            .storage()
            .and_then(|storage| StoredStatus::read(&*storage))
            .inspect(|stored| {
                info!(
                    "Pipeline deployment identifier from storage: {}",
                    stored.deployment_id
                );
                info!("Desired status from storage: {:?}", stored.desired_status);
            }) {
            _ if args.initial == RuntimeDesiredStatus::Coordination => {
                // Always defer to the coordinator if there is one.
                (
                    args.initial,
                    BootstrapConfig::from(args.bootstrap_policy)
                        .with_silent_bootstrap(args.silent_bootstrap),
                )
            }
            Some(stored) if stored.deployment_id == args.deployment_id => {
                // This is an automatic restart (otherwise the deployment ID would
                // have changed).  Use the stored desired status.
                (
                    stored.desired_status,
                    BootstrapConfig::from(stored.bootstrap_policy)
                        .with_silent_bootstrap(stored.silent_bootstrap),
                )
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
                (
                    args.initial,
                    BootstrapConfig::from(args.bootstrap_policy)
                        .with_silent_bootstrap(args.silent_bootstrap),
                )
            }
            None => {
                // Initial deployment of this pipeline.  Use the desired status
                // passed in by the pipeline manager (it's the only one we have,
                // anyway).
                (
                    args.initial,
                    BootstrapConfig::from(args.bootstrap_policy)
                        .with_silent_bootstrap(args.silent_bootstrap),
                )
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
            RuntimeDesiredStatus::Coordination
                | RuntimeDesiredStatus::Running
                | RuntimeDesiredStatus::Paused
                | RuntimeDesiredStatus::Standby,
        ) {
            return Err(ControllerError::InvalidInitialStatus(initial_status));
        }

        let host_info = args.host_id.map(|host_idx| HostInfo {
            host_idx,
            n_hosts: config.global.hosts,
        });
        let state = WebData::new(ServerState::new(
            PipelinePhase::Initializing(InitializationState::Starting),
            md,
            initial_status,
            bootstrap_config,
            args.deployment_id,
            builder.storage().clone(),
            builder.sync_config(),
            host_info,
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
                    let notify = state.desired_status_change.notified();
                    let desired_status = state.desired_status();
                    let bootstrap_config = state.bootstrap_config();
                    let stored_status = StoredStatus {
                        desired_status,
                        bootstrap_policy: bootstrap_config.active_bootstrap_policy(),
                        silent_bootstrap: bootstrap_config.silent_bootstrap,
                        deployment_id: state.deployment_id,
                    };
                    if Some(stored_status) != prev_stored_status {
                        let storage = storage.clone();
                        let _ = spawn_blocking(move || stored_status.write(&*storage)).await;
                    }
                    prev_stored_status = Some(stored_status);
                    notify.await;
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
        observability::fips::log_rustls_fips_status("HTTPS server config", server_config.fips());

        server
            .listen_rustls_0_23(listener, server_config)
            .map_err(|e| ControllerError::io_error("binding HTTPS server to the listener", e))?
            .run()
    } else {
        server
            .listen(listener)
            .map_err(|e| ControllerError::io_error("binding HTTP server to the listener", e))?
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
            .map_err(|e| ControllerError::io_error("writing server port file", e))?;
        tokio::fs::rename(&tmp_server_port_file, SERVER_PORT_FILE)
            .await
            .map_err(|e| ControllerError::io_error("renaming server port file", e))?;
        server
            .await
            .map_err(|e| ControllerError::io_error("in the HTTP server", e))
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

    if is_fatal_controller_error(&error)
        && let Ok(controller) = state.take_controller()
    {
        state.set_phase(PipelinePhase::Failed(error));
        controller.initiate_stop();
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

    // Otherwise, fall back to `INFO`
    EnvFilter::try_new("object_store=warn,buoyant_kernel=warn,info").unwrap()
}

fn do_bootstrap(
    mut builder: ControllerBuilder,
    circuit_factory: CircuitFactoryFunc,
    state: &WebData<ServerState>,
) -> Result<(), ControllerError> {
    if let Some(runtime_override) = option_env!("FELDERA_RUNTIME_OVERRIDE") {
        // - runtime_override is what the platform wants to run (it's set in an env variable
        // injected by the compiler server)
        // - actual_runtime_sha is the git SHA of the sources compiled at build-time
        //
        // If the two don't match the `runtime_version` feature isn't working properly.
        // (note that VERGEN_GIT_SHA is only set in case a custom runtime is used)
        let actual_runtime_sha = option_env!("VERGEN_GIT_SHA")
            .unwrap_or_else(|| "<unable to determine runtime git sha>");
        if runtime_override == actual_runtime_sha {
            info!(
                "Pipeline runtime version was overridden with SHA: {}",
                runtime_override,
            );
        } else {
            return Err(ControllerError::UnexpectedRuntimeVersion {
                error: format!(
                    "Pipeline runtime version was overridden but the build SHA of the binary {} does not match the requested runtime version {}",
                    actual_runtime_sha, runtime_override
                ),
            });
        }
    }

    let mut activation_warning = LongOperationWarning::new(Duration::from_secs(1));
    let mut controller_init = loop {
        match state.desired_status() {
            RuntimeDesiredStatus::Unavailable => unreachable!(),
            RuntimeDesiredStatus::Coordination => {
                if let Some(mut ca) = state.coordination_activate.lock().unwrap().take() {
                    *OUTPUT_MAPPING.lock().unwrap() = take(&mut ca.output_assignment);

                    builder = builder.with_layout(
                        Layout::new_multihost(&ca.exchanges, ca.local_address).map_err(|e| {
                            ControllerError::Config {
                                config_error: Box::new(ConfigError::InvalidLayout(e)),
                            }
                        })?,
                    );

                    match ca.desired_status {
                        RuntimeDesiredStatus::Coordination
                        | RuntimeDesiredStatus::Unavailable
                        | RuntimeDesiredStatus::Suspended => {
                            return Err(ControllerError::InvalidInitialStatus(ca.desired_status));
                        }
                        RuntimeDesiredStatus::Paused
                        | RuntimeDesiredStatus::Running
                        | RuntimeDesiredStatus::Standby => (),
                    }
                    builder.config.inputs = std::mem::take(&mut ca.inputs);
                    builder.config.outputs = std::mem::take(&mut ca.outputs);
                    *state.desired_status.lock().unwrap() = ca.desired_status;
                    match ca.checkpoint {
                        Some(checkpoint_uuid) => {
                            break builder.open_checkpoint(checkpoint_uuid);
                        }
                        None => break builder.open_without_checkpoint(),
                    }
                }
                activation_warning.check(|elapsed| {
                    warn!(
                        "Still waiting for activation from coordinator after {} seconds.",
                        elapsed.as_secs()
                    );
                });
                std::thread::sleep(Duration::from_millis(100));
            }
            RuntimeDesiredStatus::Running
            | RuntimeDesiredStatus::Paused
            | RuntimeDesiredStatus::Suspended => {
                // First, if necessary, download the latest checkpoint from S3.
                if let Some(sync) = builder.is_pull_necessary() {
                    builder.pull_once(sync)?;
                }
                break builder.open_latest_checkpoint();
            }
            RuntimeDesiredStatus::Standby => {
                state.set_phase(PipelinePhase::Initializing(InitializationState::Standby));

                builder
                    .continuous_pull(|| state.desired_status() != RuntimeDesiredStatus::Standby)?;

                let mut desired_status = state.desired_status.lock().unwrap();
                if *desired_status == RuntimeDesiredStatus::Standby {
                    warn!(
                        "Exited standby mode without specifying a new desired state, defaulting to paused"
                    );
                    *desired_status = RuntimeDesiredStatus::Paused;
                    state.desired_status_change.notify_waiters();
                }
                break builder.open_latest_checkpoint();
            }
        }
    }?;

    controller_init.set_incarnation_uuid(state.incarnation_uuid);
    let controller = controller_init.init(
        Some((**state).clone()),
        circuit_factory,
        Box::new({
            let weak_state_ref = Arc::downgrade(state);
            move |e, t| error_handler(&weak_state_ref, e, t)
        }) as Box<dyn Fn(Arc<ControllerError>, Option<String>) + Send + Sync>,
    )?;

    let desired_status = state.desired_status.lock().unwrap();
    match *desired_status {
        RuntimeDesiredStatus::Unavailable
        | RuntimeDesiredStatus::Standby
        | RuntimeDesiredStatus::Coordination => unreachable!(),
        RuntimeDesiredStatus::Paused | RuntimeDesiredStatus::Suspended => controller.pause(),
        RuntimeDesiredStatus::Running => controller.start(),
    };
    state.set_controller(controller);
    drop(desired_status);

    info!("Pipeline initialization complete");
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
        .service(start)
        .service(pause)
        .service(activate)
        .service(approve)
        .service(status_handler)
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
        .service(samply_profile)
        .service(get_samply_profile)
        .service(lir)
        .service(checkpoint)
        .service(checkpoint_status)
        .service(checkpoints)
        .service(remote_checkpoints)
        .service(checkpoint_sync)
        .service(sync_checkpoint_status)
        .service(suspend)
        .service(input_endpoint)
        .service(output_endpoint)
        .service(pause_input_endpoint)
        .service(start_input_endpoint)
        .service(input_endpoint_status)
        .service(output_endpoint_status)
        .service(output_endpoint_command)
        .service(rebalance)
        .service(start_compaction)
        .service(coordination_activate_handler)
        .service(coordination_status)
        .service(coordination_step_request)
        .service(coordination_step_status)
        .service(coordination_checkpoint_status)
        .service(coordination_checkpoint_prepare)
        .service(coordination_checkpoint_release)
        .service(coordination_checkpoint_pull)
        .service(coordination_checkpoint_pull_status)
        .service(coordination_checkpoint_push)
        .service(coordination_transaction_status)
        .service(coordination_completion_status)
        .service(coordination_adhoc_catalog)
        .service(coordination_adhoc_lease)
        .service(coordination_adhoc_scan)
        .service(coordination_labels_incomplete)
        .service(coordination_restart)
        .service(clock_advance)
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
    match args.initial {
        RuntimeDesiredStatus::Running | RuntimeDesiredStatus::Paused => {
            state_transition(state, "activate", args.initial, true).await
        }
        _ => Err(PipelineError::InvalidActivateStatus(args.initial)),
    }
}

#[post("/approve")]
async fn approve(
    state: WebData<ServerState>,
    args: Query<ApproveParameters>,
) -> Result<HttpResponse, PipelineError> {
    state.set_bootstrap_config(
        BootstrapConfig::from(BootstrapPolicy::Allow).with_silent_bootstrap(args.silent_bootstrap),
    );

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
async fn status_handler(
    state: WebData<ServerState>,
) -> Result<ExtendedRuntimeStatus, ExtendedRuntimeStatusError> {
    get_status(&state)
}

#[allow(clippy::result_large_err)]
fn get_status(state: &ServerState) -> Result<ExtendedRuntimeStatus, ExtendedRuntimeStatusError> {
    // Runtime desired status
    let runtime_desired_status = state.desired_status();

    // Storage status details
    let storage_status_details = match &state.storage {
        Some(backend) => match Checkpointer::read_checkpoints(&**backend) {
            Ok(list_checkpoints) => Some(StorageStatusDetails {
                checkpoints: list_checkpoints,
            }),
            Err(e) => {
                error!(
                    "Unable to read checkpoints; storage status details are not provided. Error: {e}"
                );
                None
            }
        },
        None => None,
    };

    // Current status
    match state.controller() {
        Ok(controller) => {
            fn inner_status(
                runtime_desired_status: RuntimeDesiredStatus,
                controller: &Controller,
                default_status: RuntimeStatus,
                storage_status_details: Option<StorageStatusDetails>,
            ) -> ExtendedRuntimeStatus {
                ExtendedRuntimeStatus {
                    runtime_status: if controller.status().bootstrap_in_progress() {
                        RuntimeStatus::Bootstrapping
                    } else if controller.is_replaying() {
                        RuntimeStatus::Replaying
                    } else {
                        default_status
                    },
                    runtime_status_details: json!(""),
                    runtime_desired_status,
                    storage_status_details,
                }
            }

            return match controller.status().global_metrics.get_state() {
                PipelineState::Paused => Ok(inner_status(
                    runtime_desired_status,
                    &controller,
                    RuntimeStatus::Paused,
                    storage_status_details,
                )),
                PipelineState::Running => Ok(inner_status(
                    runtime_desired_status,
                    &controller,
                    RuntimeStatus::Running,
                    storage_status_details,
                )),
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
                storage_status_details,
            }),
            InitializationState::DownloadingCheckpoint => Ok(ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Initializing,
                runtime_status_details: json!("downloading checkpoint from object storage"),
                runtime_desired_status,
                storage_status_details,
            }),
            InitializationState::Standby => Ok(ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Standby,
                runtime_status_details: json!(""),
                runtime_desired_status,
                storage_status_details,
            }),
            InitializationState::AwaitingApproval(diff) => Ok(ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::AwaitingApproval,
                runtime_status_details: serde_json::to_value(&diff).unwrap_or_default(),
                runtime_desired_status,
                storage_status_details,
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
                    storage_status_details,
                })
            } else {
                let mut status_code = e.status_code();
                if status_code == StatusCode::SERVICE_UNAVAILABLE {
                    error!(
                        "Endpoint /status: unexpected status code {status_code} has been converted into {} for error {e}",
                        StatusCode::SERVICE_UNAVAILABLE
                    );
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
            storage_status_details,
        }),
    }
}

/// Retrieve whether a pipeline is suspendable or not.
#[get("/suspendable")]
async fn suspendable(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    let reasons = match state.controller()?.can_suspend() {
        Err(SuspendError::Permanent(errors)) => Some(errors),
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
    let controller = state.controller()?;
    if !request_is_websocket(&request) {
        stream_adhoc_result(&controller, &args).await
    } else {
        adhoc_websocket(controller, request, stream).await
    }
}

#[derive(Debug, Default, Deserialize)]
struct StatsParams {
    /// When `true`, include the most recent error messages for each endpoint
    /// in the response (up to `MAX_CONNECTOR_ERRORS` per list). Default is
    /// `false` so that callers polling `/stats` keep getting a lightweight
    /// response. This selector is intended for the support-bundle collector.
    #[serde(default)]
    include_connector_errors: bool,
}

#[get("/stats")]
async fn stats(
    state: WebData<ServerState>,
    params: Query<StatsParams>,
) -> Result<HttpResponse, PipelineError> {
    let include_connector_errors = params.into_inner().include_connector_errors;
    Ok(HttpResponse::Ok().json(state.controller()?.api_status(include_connector_errors)))
}

/// This endpoint returns a subset of stats that don't need updating and so is more performant than /stats
#[get("/stats/errors")]
async fn error_stats(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    let controller = state.controller()?;
    let controller_status = controller.status();

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
    #[cfg(all(target_os = "linux", feature = "with-heap-profiling"))]
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
    #[cfg(not(all(target_os = "linux", feature = "with-heap-profiling")))]
    {
        Err::<HttpResponse, PipelineError>(PipelineError::HeapProfilerError {
            error: "heap profiling is not available in this build".to_string(),
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

#[post("/samply_profile")]
async fn samply_profile(
    state: WebData<ServerState>,
    query_params: web::Query<SamplyProfileParams>,
) -> Result<HttpResponse, PipelineError> {
    let duration = query_params.duration_secs;
    let controller = state.controller()?;

    let state_samply_state = state.samply_state.clone();

    // Check if profiling is already in progress
    {
        let samply_state = state_samply_state.lock().unwrap();
        if matches!(samply_state.samply_status, SamplyStatus::InProgress { .. }) {
            return Ok(HttpResponse::Conflict().json(ErrorResponse {
                message: "samply profile collection is already in progress".to_string(),
                error_code: "SamplyProfilingInProgress".into(),
                details: serde_json::Value::Null,
            }));
        }
    }

    // Set the state to InProgress with expected completion time
    let expected_after = chrono::Utc::now() + chrono::Duration::seconds(duration as i64);
    state_samply_state
        .lock()
        .unwrap()
        .start_profiling(expected_after);

    let layout = controller.layout();
    let os_cpu = layout
        .is_multihost()
        .then(|| format!("host {} of {}", layout.local_host_idx(), layout.n_hosts()));
    spawn(async move {
        let result = controller
            .async_samply_profile(duration, os_cpu.as_deref())
            .await;
        state_samply_state
            .lock()
            .unwrap()
            .complete_profiling(result);
    });

    // Wait to check if it errored out immediately
    sleep(Duration::from_millis(600)).await;

    // Check if profiling is still running or failed immediately
    let samply_state = state.samply_state.lock().unwrap();
    Ok(match samply_state.samply_status {
        // Profile is still running - return success
        SamplyStatus::InProgress { .. } => HttpResponse::Accepted().finish(),
        // Profile completed during wait - check if it failed
        SamplyStatus::Idle => match &samply_state.last_profile {
            Some(Err(error)) => samply_profile_error_response(error),
            _ => HttpResponse::InternalServerError().json(ErrorResponse {
                message: "samply profiling completed unexpectedly".to_string(),
                error_code: "SamplyProfilingUnexpectedCompletion".into(),
                details: serde_json::Value::Null,
            }),
        },
    })
}

#[get("/samply_profile")]
async fn get_samply_profile(
    state: WebData<ServerState>,
    query_params: web::Query<SamplyProfileGetParams>,
) -> Result<HttpResponse, PipelineError> {
    let samply_state = state.samply_state.lock().unwrap();

    // If latest=true, check if profiling is in progress and return 204 No Content
    if query_params.latest
        && let SamplyStatus::InProgress { expected_after } = samply_state.samply_status
    {
        let now = chrono::Utc::now();
        let retry_after_secs = (expected_after - now).num_seconds().max(0);

        return Ok(HttpResponse::NoContent()
            .insert_header(("Retry-After", retry_after_secs.to_string()))
            .finish());
    }

    // Return the last profile result
    Ok(samply_profile_response(&samply_state.last_profile))
}

/// Helper function to construct error response for samply profiling failure
fn samply_profile_error_response(error: &str) -> HttpResponse {
    HttpResponse::InternalServerError().json(ErrorResponse {
        message: "failed to profile the pipeline using samply".to_string(),
        error_code: "SamplyProfilingFailure".into(),
        details: serde_json::Value::String(error.to_string()),
    })
}

/// Helper function to construct HttpResponse from SamplyProfile result
fn samply_profile_response(last_profile: &SamplyProfile) -> HttpResponse {
    match last_profile {
        Some(Ok(bytes)) => {
            let bytes = bytes.clone();
            let byte_stream = once(async move { Ok::<_, PipelineError>(web::Bytes::from(bytes)) });

            HttpResponse::Ok()
                .content_type("application/gzip")
                .insert_header((
                    "Content-Disposition",
                    format!(
                        "attachment; filename=\"{}-samply_profile.json.gz\"",
                        chrono::Local::now().to_rfc3339()
                    ),
                ))
                .streaming(byte_stream)
        }
        Some(Err(error)) => samply_profile_error_response(error),
        None => HttpResponse::BadRequest().json(json!({
            "message": "no samply profile found; trigger a samply profile by making a POST request to `/samply_profile`",
            "error_code": "NoSamplyProfile",
            "details": null
        })),
    }
}

fn get_checkpoints(state: &ServerState) -> Result<VecDeque<CheckpointMetadata>, PipelineError> {
    Ok(match &state.storage {
        Some(backend) => {
            Checkpointer::read_checkpoints(&**backend).map_err(ControllerError::dbsp_error)?
        }
        None => Default::default(),
    })
}

#[post("/checkpoint/sync")]
async fn checkpoint_sync(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    let controller = state.controller()?;

    if controller.layout().is_multihost() {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            message: "checkpoint sync is not supported directly on multihost pipelines; \
                      sync requests must go through the coordinator via \
                      `/coordination/checkpoint/push`"
                .to_string(),
            error_code: "400".into(),
            details: serde_json::Value::Null,
        }));
    }

    let Some(last_checkpoint) = get_checkpoints(&state)?.back().map(|c| c.uuid) else {
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

/// Request body for `POST /coordination/checkpoint/push`.
#[derive(Deserialize)]
struct CoordinationPushBody {
    /// UUID of the local checkpoint to push to object storage.
    uuid: Uuid,
}

/// Triggers a push of a specific checkpoint to object storage.
///
/// Called by the multihost coordinator to direct each pod to sync a particular
/// checkpoint UUID.  The coordinator selects the same logical step for all pods
/// before calling this endpoint, ensuring all pods' remote catalogs converge
/// on a consistent snapshot.
#[post("/coordination/checkpoint/push")]
async fn coordination_checkpoint_push(
    state: WebData<ServerState>,
    body: web::Json<CoordinationPushBody>,
) -> Result<HttpResponse, PipelineError> {
    let uuid = body.into_inner().uuid;
    let controller = state.controller()?;

    if get_checkpoints(&state)?.iter().all(|c| c.uuid != uuid) {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            message: format!("checkpoint '{uuid}' not found in local storage"),
            error_code: "400".into(),
            details: serde_json::Value::Null,
        }));
    }

    spawn(async move {
        let result = controller.async_sync_checkpoint(uuid).await;
        state
            .sync_checkpoint_state
            .lock()
            .unwrap()
            .completed(uuid, result);
    });

    Ok(HttpResponse::Accepted().json(CheckpointSyncResponse::new(uuid)))
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
    let status = state.checkpoint_state.lock().unwrap().status.clone();
    HttpResponse::Ok().json(status)
}

#[get("/checkpoints")]
async fn checkpoints(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    Ok(HttpResponse::Ok().json(get_checkpoints(&state)?))
}

/// List checkpoints available in the configured remote object storage.
#[get("/checkpoints/remote")]
async fn remote_checkpoints(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    let sync = state
        .sync_config
        .clone()
        .ok_or_else(|| PipelineError::ControllerError {
            error: Arc::new(ControllerError::checkpoint_fetch_error(
                "listing remote checkpoints requires sync to be configured".to_string(),
            )),
        })?;

    let result = spawn_blocking(move || crate::controller::sync::list_remote_checkpoints(&sync))
        .await
        .map_err(|e| PipelineError::ControllerError {
            error: Arc::new(ControllerError::checkpoint_fetch_error(format!("{e}"))),
        })?
        .map_err(|e| PipelineError::ControllerError { error: Arc::new(e) })?;

    Ok(HttpResponse::Ok().json(result))
}

#[get("/checkpoint/sync_status")]
async fn sync_checkpoint_status(
    state: WebData<ServerState>,
) -> Result<HttpResponse, PipelineError> {
    let mut sync_state = state.sync_checkpoint_state.lock().unwrap();

    let controller = state.controller()?;
    if let Some(chk) = controller.last_checkpoint_sync().id {
        sync_state.completed_periodic(chk);
    }

    Ok(HttpResponse::Ok().json(sync_state.status.clone()))
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
            return Err(PipelineError::InvalidTransition("suspend", *desired_status));
        }
        RuntimeDesiredStatus::Coordination
        | RuntimeDesiredStatus::Running
        | RuntimeDesiredStatus::Paused => {
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
                if let Ok(controller) = state.take_controller()
                    && let Err(error) = controller.async_stop().await
                {
                    error!("stopping controller failed ({error})");
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

/// Advance the externally-driven `NOW()` clock and return its new value.
///
/// Requires `dev_tweaks.now_http_driven = true`.
#[post("/clock/advance")]
async fn clock_advance(
    state: WebData<ServerState>,
    body: web::Json<feldera_types::transport::clock::ClockAdvanceRequest>,
) -> Result<impl Responder, PipelineError> {
    use feldera_types::transport::clock::ClockAdvanceResponse;
    let controller = state.controller()?;
    let reader =
        controller
            .get_input_endpoint("now")
            .ok_or_else(|| PipelineError::InvalidParam {
                error: "this pipeline's program does not reference `NOW()`; \
                        the clock connector is not active and cannot be advanced"
                    .to_string(),
            })?;
    let clock_reader = reader
        .as_any()
        .downcast::<crate::transport::clock::ClockReader>()
        .map_err(|_| PipelineError::InvalidParam {
            error: "the `now` input endpoint is not the built-in clock connector".to_string(),
        })?;

    let now_ms =
        clock_reader
            .advance(body.delta_ms)
            .await
            .map_err(|e| PipelineError::InvalidParam {
                error: e.to_string(),
            })?;

    let now = chrono::DateTime::<Utc>::from_timestamp_millis(now_ms)
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_else(|| format!("{now_ms} ms since epoch"));

    Ok(HttpResponse::Ok().json(ClockAdvanceResponse { now_ms, now }))
}

#[derive(Debug, Deserialize)]
struct IngressArgs {
    #[serde(default = "HttpInputTransport::default_format")]
    format: String,
    /// Push data to the pipeline even if the pipeline is in a paused state.
    #[serde(default)]
    force: bool,
}

/// Lookup or create an HTTP input endpoint.
async fn get_or_create_http_input_endpoint(
    state: &WebData<ServerState>,
    format: FormatConfig,
    table_name: String,
    endpoint_name: String,
) -> Result<HttpInputEndpoint, PipelineError> {
    let controller = state.controller()?;

    // We rely on the name to uniquely encode connector configuration.
    if let Some(reader) = controller.get_input_endpoint(&endpoint_name)
        && let Ok(endpoint) = reader.as_any().downcast::<HttpInputEndpoint>()
    {
        return Ok(endpoint.as_ref().clone());
    }

    let config = HttpInputConfig {
        name: endpoint_name.clone(),
    };

    let guard = controller.register_api_connection()?;
    let endpoint = HttpInputEndpoint::new(config.clone()).with_api_connection_guard(guard);
    // Create endpoint config.
    let config = InputEndpointConfig::new(
        table_name,
        ConnectorConfig::new(TransportConfig::HttpInput(config), Some(format))
            .with_max_queued_records(HttpInputTransport::default_max_buffered_records()),
    );

    controller
        .add_input_endpoint(
            &endpoint_name,
            config,
            Box::new(endpoint.clone()) as Box<dyn TransportInputEndpoint>,
            None,
        )
        .inspect_err(|e| {
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
    // A local cache of HTTP input endpoints. We create one endpoint per (table_name, format) pair
    // in the controller. Caching them in a thread-local variable avoids acquiring the global controller
    // lock on every HTTP request.
    thread_local! {
        static TABLE_ENDPOINTS: RefCell<HashMap<String, HttpInputEndpoint, BuildHasherDefault<DefaultHasher>>> = const {
            RefCell::new(HashMap::with_hasher(BuildHasherDefault::new()))
        };
    }
    debug!("{req:?}");

    let table_name = path.into_inner();

    // Generate deterministic endpoint name per (table_name, FormatConfig).
    let parser_endpoint_name = format!("{table_name}.api-ingress-{}", args.format);
    let format = parser_config_from_http_request(&parser_endpoint_name, &args.format, &req)?;

    let mut endpoint_hasher = DefaultHasher::new();
    table_name.hash(&mut endpoint_hasher);
    format.hash(&mut endpoint_hasher);
    let endpoint_hash = endpoint_hasher.finish();

    let endpoint_name = format!("{table_name}.api-ingress-{endpoint_hash:016x}");

    let cached_endpoint =
        TABLE_ENDPOINTS.with(|endpoints| endpoints.borrow().get(&endpoint_name).cloned());
    let endpoint = match cached_endpoint {
        Some(endpoint) => endpoint,
        None => {
            let endpoint = get_or_create_http_input_endpoint(
                &state,
                format.clone(),
                table_name.clone(),
                endpoint_name.clone(),
            )
            .await?;
            TABLE_ENDPOINTS
                .with_borrow_mut(|endpoints| endpoints.insert(endpoint_name, endpoint.clone()));
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
    format: HttpOutputFormat,
    request: &HttpRequest,
) -> Result<FormatConfig, ControllerError> {
    let format = match format {
        HttpOutputFormat::Csv => {
            Box::new(CsvOutputFormat) as Box<dyn feldera_adapterlib::format::OutputFormat>
        }
        HttpOutputFormat::Json => Box::new(JsonOutputFormat),
    };

    let config = format.config_from_http_request(endpoint_name, request)?;

    Ok(FormatConfig {
        name: format.name(),
        config: serde_json::to_value(config)
            .map_err(|e| ControllerError::encoder_config_parse_error(endpoint_name, &e, ""))?,
    })
}

/// URL-encoded arguments to the `/egress` endpoint.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
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
    backpressure: Option<bool>,

    /// Data format used to encode the output of the query.
    format: Option<HttpOutputFormat>,

    /// When `true`, deliver a full snapshot of the materialized view before
    /// streaming incremental updates. The view must be materialized.
    #[serde(default)]
    send_snapshot: bool,
}

#[post("/egress/{table_name}")]
async fn output_endpoint(
    state: WebData<ServerState>,
    path: web::Path<String>,
    req: HttpRequest,
    args: Query<EgressArgs>,
    body: web::Bytes,
) -> Result<HttpResponse, PipelineError> {
    debug!("/egress request:{req:?}");

    let table_name = path.into_inner();

    let connector_config = if !body.is_empty() {
        let mut json = serde_json::from_slice::<serde_json::Value>(&body).map_err(|e| {
            PipelineError::InvalidParam {
                error: e.to_string(),
            }
        })?;

        if args.format.is_some() || args.backpressure.is_some() {
            return Err(PipelineError::InvalidParam {
                error: String::from(
                    "Can't mix configuration between JSON body and query parameters",
                ),
            });
        }

        if let Some(object) = json.as_object_mut()
            && !object.contains_key("transport")
        {
            object.insert(
                String::from("transport"),
                serde_json::to_value(TransportConfig::HttpOutput(HttpOutputConfig::default()))
                    .unwrap(),
            );
        }
        serde_json::from_value(json).map_err(|e| PipelineError::InvalidParam {
            error: e.to_string(),
        })?
    } else {
        let format =
            encoder_config_from_http_request(&table_name, args.format.unwrap_or_default(), &req)?;
        let mut connector_config = ConnectorConfig::new(
            TransportConfig::HttpOutput(HttpOutputConfig {
                backpressure: args.backpressure.unwrap_or_default(),
            }),
            Some(format),
        )
        .with_max_queued_records(HttpOutputTransport::default_max_buffered_records());
        connector_config.send_snapshot = args.send_snapshot;
        connector_config
    };
    let config = OutputEndpointConfig::new(table_name, connector_config);

    let http_output_config = match &config.connector_config.transport {
        TransportConfig::HttpOutput(config) => config.clone(),
        _ => {
            return Err(PipelineError::InvalidParam {
                error: "Transport configuration must be `http_output`".to_string(),
            });
        }
    };
    let format = match &config.connector_config.format {
        Some(format) if format.name == "csv" => HttpOutputFormat::Csv,
        Some(format) if format.name == "json" => {
            let json_format: JsonEncoderConfig = serde_json::from_value(format.config.clone())
                .map_err(|e| PipelineError::InvalidParam {
                    error: format!("Invalid JSON format configuration: {e}"),
                })?;
            if !json_format.array {
                return Err(PipelineError::InvalidParam {
                    error: String::from(r#"JSON format configuration must set `"array": true`"#),
                });
            }
            HttpOutputFormat::Json
        }
        _ => {
            return Err(PipelineError::InvalidParam {
                error: "Format must be configured as `csv` or `json`".to_string(),
            });
        }
    };

    // Create HTTP endpoint.
    let endpoint_name = format!("{}.api-{}", &config.stream, Uuid::new_v4());
    let endpoint = HttpOutputEndpoint::new(&endpoint_name, format, http_output_config.backpressure);
    // Pre-connect the streaming receiver before `add_output_endpoint` so the
    // initial snapshot (when `send_snapshot` is enabled) is captured rather
    // than racing the streaming body that drains it.
    let response_receiver = endpoint.connect_stream();

    // Connect endpoint.
    let controller = state.controller()?;
    let _guard = controller.register_api_connection()?;

    let endpoint_id = controller.add_output_endpoint(
        &endpoint_name,
        &config,
        Box::new(endpoint.clone()) as Box<dyn OutputEndpoint>,
        None,
    )?;

    // We need to pass a callback to `request` to disconnect the endpoint when the
    // request completes.  Use a downgraded reference to `state`, so
    // this closure doesn't prevent the controller from shutting down.
    let weak_state = Arc::downgrade(&state);

    // Call endpoint to create a response with a streaming body, which will be
    // evaluated after we return the response object to actix.
    Ok(endpoint.request(
        Some(response_receiver),
        Box::new(move || {
            // Delete endpoint on completion/error.
            // We don't control the lifetime of the response object after
            // returning it to actix, so the only way to run cleanup code
            // when the HTTP request terminates is to piggyback on the
            // destructor.
            if let Some(state) = weak_state.upgrade()
                && let Ok(controller) = state.controller()
            {
                controller.disconnect_output(&endpoint_id);
            }
        }),
    ))
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

#[post("/output_endpoints/{endpoint_name}/command")]
async fn output_endpoint_command(
    state: WebData<ServerState>,
    path: web::Path<String>,
    command: web::Json<serde_json::Value>,
) -> Result<HttpResponse, PipelineError> {
    Ok(HttpResponse::Ok().json(
        state
            .controller()?
            .output_endpoint_command(&path, command.into_inner())?,
    ))
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

    let controller = state.controller()?;
    Ok(HttpResponse::Ok().json(CompletionStatusResponse::new(
        controller
            .completion_status(&token)
            .map_err(|e| state.maybe_missing_controller_error(e))?,
        controller.status().global_metrics.total_completed_steps(),
    )))
}

#[post("/rebalance")]
async fn rebalance(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    state.controller()?.rebalance().await?;

    Ok(HttpResponse::Ok().into())
}

#[post("/start_compaction")]
async fn start_compaction(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    state.controller()?.start_compaction().await?;
    Ok(HttpResponse::Ok().into())
}

#[post("/coordination/activate")]
async fn coordination_activate_handler(
    state: WebData<ServerState>,
    args: web::Json<CoordinationActivate>,
) -> Result<HttpResponse, PipelineError> {
    *state.coordination_activate.lock().unwrap() = Some(args.into_inner());
    state.desired_status_change.notify_waiters();
    Ok(HttpResponse::Ok().finish())
}

#[get("/coordination/status")]
async fn coordination_status(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    let stream = unfold((state, None), |(state, prev)| async move {
        let status = match prev {
            None => get_status(&state),
            Some(prev) => loop {
                let notify = state.desired_status_change.notified();
                let status = get_status(&state);
                if status != prev {
                    break status;
                }

                // We put a 1-second timeout on this because the status can
                // change without desired_status_change being notified in two cases:
                //
                // - Bootstrapping has completed.
                //
                // - Replaying is completed.
                //
                // Either of these can only happen at most once per pipeline run
                // (and they can't both happen), and it's probably OK that the
                // notification is delayed a bit.  (If it turns out that prompt
                // notification is important, then we can arrange for that.)
                let _ = timeout(Duration::from_secs(1), notify).await;
            },
        };
        Some((
            CoordinationStatus {
                incarnation_uuid: state.incarnation_uuid,
                status: status.clone(),
            },
            (state, Some(status)),
        ))
    });
    Ok(
        HttpResponseBuilder::new(StatusCode::OK).streaming(stream.map(|value| {
            Ok::<_, Infallible>(Bytes::from(serde_json::to_string(&value).unwrap() + "\n"))
        })),
    )
}

#[post("/coordination/step/request")]
async fn coordination_step_request(
    state: WebData<ServerState>,
    args: web::Json<StepRequest>,
) -> Result<HttpResponse, PipelineError> {
    state.controller()?.set_coordination_request(args.clone());
    Ok(HttpResponse::Ok().finish())
}

#[get("/coordination/step/status")]
async fn coordination_step_status(
    state: WebData<ServerState>,
) -> Result<HttpResponse, PipelineError> {
    Ok(HttpResponseBuilder::new(StatusCode::OK).streaming(
        WatchStream::new(state.controller()?.step_watcher()).map(|value| {
            Ok::<_, Infallible>(Bytes::from(serde_json::to_string(&value).unwrap() + "\n"))
        }),
    ))
}

#[get("/coordination/checkpoint/status")]
async fn coordination_checkpoint_status(
    state: WebData<ServerState>,
) -> Result<HttpResponse, PipelineError> {
    Ok(HttpResponseBuilder::new(StatusCode::OK).streaming(
        WatchStream::new(state.controller()?.checkpoint_watcher()).filter_map(|value| async {
            value.map(|value| {
                Ok::<_, Infallible>(Bytes::from(serde_json::to_string(&value).unwrap() + "\n"))
            })
        }),
    ))
}

#[post("/coordination/checkpoint/prepare")]
async fn coordination_checkpoint_prepare(
    state: WebData<ServerState>,
) -> Result<HttpResponse, PipelineError> {
    state.controller()?.prepare_checkpoint();
    Ok(HttpResponse::Ok().finish())
}

#[post("/coordination/checkpoint/release")]
async fn coordination_checkpoint_release(
    state: WebData<ServerState>,
) -> Result<HttpResponse, PipelineError> {
    state.controller()?.release_checkpoint();
    Ok(HttpResponse::Ok().finish())
}

/// Request body for `POST /coordination/checkpoint/pull`.
#[derive(Deserialize)]
struct CoordinationPullBody {
    #[serde(default)]
    standby: bool,
}

/// Pulls the latest checkpoint from object storage into local storage.
///
/// Returns 202 Accepted and starts a background pull.  If a pull is already in
/// progress, returns 200 OK without starting a new one.  Poll
/// `GET /coordination/checkpoint/pull_status` for the result.  Fails
/// synchronously only if the pipeline is already running or storage/sync config
/// is absent.
#[post("/coordination/checkpoint/pull")]
async fn coordination_checkpoint_pull(
    state: WebData<ServerState>,
    body: web::Json<CoordinationPullBody>,
) -> Result<HttpResponse, PipelineError> {
    if matches!(state.phase(), PipelinePhase::InitializationComplete) {
        return Err(PipelineError::ControllerError {
            error: Arc::new(ControllerError::checkpoint_fetch_error(
                "checkpoint pull is not allowed while the pipeline is already running".to_string(),
            )),
        });
    }

    let storage = state
        .storage
        .clone()
        .ok_or_else(|| PipelineError::ControllerError {
            error: Arc::new(ControllerError::checkpoint_fetch_error(
                "checkpoint pull requires storage to be configured".to_string(),
            )),
        })?;
    let sync = state
        .sync_config
        .clone()
        .ok_or_else(|| PipelineError::ControllerError {
            error: Arc::new(ControllerError::checkpoint_fetch_error(
                "checkpoint pull requires sync to be configured".to_string(),
            )),
        })?;

    let host_info = state.host_info;
    let standby = body.into_inner().standby;

    {
        let mut pull_state = state.pull_state.lock().unwrap();
        if matches!(*pull_state, CheckpointPullStatus::InProgress) {
            return Ok(HttpResponse::Ok().finish());
        }
        *pull_state = CheckpointPullStatus::InProgress;
    }
    info!("coordination checkpoint pull: host_info={host_info:?} standby={standby}");

    spawn(async move {
        let result = spawn_blocking(move || {
            crate::controller::sync::pull_once_with_backend(storage, &sync, host_info, standby)
        })
        .await
        .unwrap();

        let new_status = match result {
            Ok(()) => {
                info!("coordination checkpoint pull: done");
                CheckpointPullStatus::Ok
            }
            Err(e) => {
                error!("coordination checkpoint pull failed: {e:?}");
                CheckpointPullStatus::Error {
                    error: e.to_string(),
                }
            }
        };
        *state.pull_state.lock().unwrap() = new_status;
    });

    Ok(HttpResponse::Accepted().finish())
}

/// Returns the status of the most recent `POST /coordination/checkpoint/pull`.
///
/// Returns one of:
/// - `{"status": "not_requested"}` — no pull has been requested yet.
/// - `{"status": "in_progress"}` — a pull is currently running.
/// - `{"status": "ok"}` — the pull completed successfully.
/// - `{"status": "error", "error": "..."}` — the pull failed.
#[get("/coordination/checkpoint/pull_status")]
async fn coordination_checkpoint_pull_status(state: WebData<ServerState>) -> HttpResponse {
    HttpResponse::Ok().json(state.pull_state.lock().unwrap().clone())
}

#[get("/coordination/transaction/status")]
async fn coordination_transaction_status(
    state: WebData<ServerState>,
) -> Result<HttpResponse, PipelineError> {
    Ok(HttpResponseBuilder::new(StatusCode::OK).streaming(
        WatchStream::new(state.controller()?.transaction_watcher()).map(|value| {
            Ok::<_, Infallible>(Bytes::from(serde_json::to_string(&value).unwrap() + "\n"))
        }),
    ))
}

#[get("/coordination/completion/status")]
async fn coordination_completion_status(
    state: WebData<ServerState>,
) -> Result<HttpResponse, PipelineError> {
    Ok(HttpResponseBuilder::new(StatusCode::OK).streaming(
        WatchStream::new(state.controller()?.completion_watcher()).map(|value| {
            Ok::<_, Infallible>(Bytes::from(serde_json::to_string(&value).unwrap() + "\n"))
        }),
    ))
}

#[get("/coordination/adhoc/catalog")]
async fn coordination_adhoc_catalog(
    state: WebData<ServerState>,
) -> Result<HttpResponse, PipelineError> {
    Ok(HttpResponse::Ok().json(state.controller()?.adhoc_catalog()))
}

#[post("/coordination/adhoc/lease")]
async fn coordination_adhoc_lease(
    state: WebData<ServerState>,
    step: web::Json<Step>,
) -> Result<HttpResponse, PipelineError> {
    let step = *step;

    // Grab the snapshot for the step.
    let controller = state.controller()?;
    let snapshot = match controller.consistent_snapshot(step).await {
        Some(snapshot) => snapshot,
        None => {
            // INSTRUMENTATION: the coordinator leased `step` (its `current_step`),
            // but our snapshot for it has already been evicted. Log which steps we
            // still retain so we can see how far `current_step` lagged this host.
            let retained = controller.available_snapshot_steps().await;
            error!(
                "adhoc lease: snapshot for step {step} is not available; \
                 retained snapshot steps = {retained:?} (this host is at step \
                 {:?})",
                retained.last()
            );
            return Err(PipelineError::AdHocQueryError {
                error: format!(
                    "snapshot for step {step} is not available (retained steps: {retained:?})"
                ),
                df: None,
            });
        }
    };

    // Add the snapshot to the table of leases.
    state
        .leases
        .lock()
        .unwrap()
        .entry(step)
        .or_insert(Lease {
            reference_count: 0,
            snapshot,
        })
        .reference_count += 1;

    // Prepare a drop guard to remove the snapshot from the table when the
    // client closes the connection.
    struct DropGuard {
        state: WebData<ServerState>,
        step: Step,
    }
    impl Drop for DropGuard {
        fn drop(&mut self) {
            if let Entry::Occupied(mut occupied) =
                self.state.leases.lock().unwrap().entry(self.step)
            {
                occupied.get_mut().reference_count -= 1;
                if occupied.get().reference_count == 0 {
                    occupied.remove();
                }
            } else {
                unreachable!()
            }
        }
    }
    let drop_guard = DropGuard {
        state: state.clone(),
        step,
    };

    // Prepare a stream to send the client.
    //
    // Really, we should only have to send a single response, then hang.
    // However, actix-web doesn't like that; it seems that it only checks for
    // connection drop when there is some data to send, which might be related
    // to [bug 1313].
    //
    // We do get prompt enough dropping if we send something once a second or
    // so.  There's not much reason to try to drop the leases faster than that,
    // since not much in the way of changes can accumulate in just a second.
    //
    // [bug 1313]: https://github.com/actix/actix-web/issues/1313
    let stream = once(async { Ok::<_, PipelineError>(Bytes::from("OK\n")) }).chain(unfold(
        drop_guard,
        move |drop_guard| async move {
            sleep(Duration::from_secs(1)).await;
            Some((Ok(Bytes::from(format!("Leased step {step}\n"))), drop_guard))
        },
    ));

    // Return a streaming response.  The client can close the stream to drop the
    // lease.
    Ok(HttpResponseBuilder::new(StatusCode::OK).streaming(stream))
}

#[post("/coordination/adhoc/scan")]
async fn coordination_adhoc_scan(
    state: WebData<ServerState>,
    scan: web::Json<AdHocScan>,
) -> Result<HttpResponse, PipelineError> {
    // Get the snapshot for the specified step.
    let snapshot = state
        .leases
        .lock()
        .unwrap()
        .get(&scan.step)
        .ok_or_else(|| PipelineError::AdHocQueryError {
            error: format!("no lease for step {}", scan.step),
            df: None,
        })?
        .snapshot
        .clone();

    // Get the scan results as a stream.
    let controller = state.controller()?;
    let session_context = controller.session_context()?;
    let mut session_state = session_context.state();
    set_snapshot(&mut session_state, snapshot);
    let table = session_context
        .table_provider(scan.table.sql_name())
        .await?;
    let execution = table
        .scan(&session_state, scan.projection.as_ref(), &[], None)
        .await?;
    let mut stream = execution.execute(
        scan.worker - controller.layout().local_workers().start,
        session_state.task_ctx(),
    )?;

    // Take the first batch out of the stream and report an error if there was
    // one.  After the first batch, there is no way to properly report an error,
    // so this at least allows us to report errors at the point they are most
    // likely.
    //
    // Crucially, an error that occurs *after* the first batch can no longer
    // change the HTTP status (200 OK and the initial bytes are already on the
    // wire). All we can do is stop writing, which truncates the Arrow stream;
    // the coordinator then observes a generic "Unexpected End of Stream" with no
    // hint as to the real cause. To keep that cause from being lost, we log it
    // here -- with table/step/worker context -- before the stream terminates.
    let first_batch = match stream.next().await {
        Some(Err(error)) => return Err(error.into()),
        other => other.into_iter(),
    };

    let schema = stream.schema();

    // Diagnostic context, captured so it can be logged from inside the stream.
    let table = scan.table.clone();
    let step = scan.step;
    let worker = scan.worker;

    let response_stream = async_stream::stream! {
        let mut writer = match StreamWriter::try_new(Vec::new(), &schema) {
            Ok(writer) => writer,
            Err(error) => {
                error!(
                    "ad-hoc scan of {table} (step {step}, worker {worker}): failed to \
                     create the Arrow stream writer: {error}"
                );
                yield Err(error.into());
                return;
            }
        };
        let mut stream = futures_util::stream::iter(first_batch).chain(stream);
        while let Some(batch) = stream.next().await {
            let batch = match batch {
                Ok(batch) => batch,
                Err(error) => {
                    error!(
                        "ad-hoc scan of {table} (step {step}, worker {worker}): error \
                         producing a record batch after streaming had already begun; the \
                         response will be truncated and the coordinator will report an \
                         incomplete Arrow stream: {error}"
                    );
                    yield Err(error.into());
                    return;
                }
            };
            if let Err(error) = writer.write(&batch) {
                error!(
                    "ad-hoc scan of {table} (step {step}, worker {worker}): error encoding \
                     a record batch into the Arrow stream: {error}"
                );
                yield Err(error.into());
                return;
            }
            yield Ok(Bytes::copy_from_slice(writer.get_ref().as_slice()));
            writer.get_mut().clear();
        }
        match writer.into_inner() {
            Ok(buffer) => yield Ok(Bytes::from(buffer)),
            Err(error) => {
                error!(
                    "ad-hoc scan of {table} (step {step}, worker {worker}): error \
                     finalizing the Arrow stream: {error}"
                );
                yield Err(error.into());
            }
        }
    };
    Ok(HttpResponseBuilder::new(StatusCode::OK).streaming::<_, PipelineError>(response_stream))
}

/// Stream the set of incomplete labels.
#[get("/coordination/labels/incomplete")]
async fn coordination_labels_incomplete(
    state: WebData<ServerState>,
) -> Result<HttpResponse, PipelineError> {
    let controller = state.controller()?;
    let notify = controller.input_completion_notify();

    let response_stream = async_stream::stream! {
        loop {
            let notify = notify.notified();
            let labels = format!("{}\n", serde_json::to_string(&Labels {
                incomplete: controller.incomplete_labels()
            }).unwrap());
            yield Ok::<_, actix_web::Error>(web::Bytes::from(labels));
            notify.await;
        }
    };

    Ok(HttpResponse::Ok()
        .content_type("application/x-ndjson")
        .streaming(response_stream))
}

#[post("/coordination/restart")]
async fn coordination_restart(
    state: WebData<ServerState>,
    args: web::Json<RestartArgs>,
) -> Result<HttpResponse, PipelineError> {
    if state.incarnation_uuid != args.incarnation_uuid {
        return Err(PipelineError::IncarnationUuidMismatch {
            requested: args.incarnation_uuid,
            expected: state.incarnation_uuid,
        });
    }

    tokio::spawn(async move {
        // Sleep for a second to allow the successful reply to (usually)
        // propagate to the requester.
        sleep(Duration::from_secs(1)).await;

        // Exit the process with a special code to tell the supervisor to
        // restart us.
        std::process::exit(55);
    });

    info!("restarting pipeline due to coordinator request");
    Ok(HttpResponse::Accepted().json("Pipeline is restarting"))
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct StoredStatus {
    /// Desired status.
    desired_status: RuntimeDesiredStatus,

    /// Bootstrap policy.
    bootstrap_policy: BootstrapPolicy,

    /// Bootstrap the pipeline with output connectors disabled.
    #[serde(default, skip_serializing_if = "is_false")]
    silent_bootstrap: bool,

    // Deployment ID.
    deployment_id: Uuid,
}

fn is_false(value: &bool) -> bool {
    !value
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

/// Helpers shared between the HTTP-only and Kafka-using server tests. Lives
/// outside `mod test_with_kafka` so it stays available when `with-kafka` is
/// off.
#[cfg(test)]
mod test_http_helpers {
    use super::{ServerArgs, ServerState, bootstrap, build_app, parse_config};
    use crate::{
        controller::ControllerBuilder,
        server::{InitializationState, PipelinePhase},
        test::{TestStruct, async_wait, http::TestHttpSender, test_circuit},
    };
    use actix_test::{PayloadError, TestServer};
    use actix_web::{
        App,
        http::StatusCode,
        middleware::Logger,
        web::{self, Data as WebData},
    };
    use csv::ReaderBuilder as CsvReaderBuilder;
    use dbsp::storage::dirlock::LockedDirectory;
    use feldera_types::adapter_stats::{ExternalControllerStatus, ExternalOutputEndpointMetrics};
    use feldera_types::runtime_status::{
        BootstrapConfig, ExtendedRuntimeStatus, RuntimeDesiredStatus,
    };
    use feldera_types::{
        completion_token::{CompletionStatus, CompletionStatusResponse, CompletionTokenResponse},
        runtime_status::{BootstrapPolicy, RuntimeStatus},
    };
    use futures::{Stream, StreamExt};
    use std::{
        fs::File,
        io::Write,
        path::Path,
        thread,
        thread::sleep,
        time::{Duration, Instant},
    };
    use tempfile::NamedTempFile;
    use uuid::Uuid;

    pub(super) async fn get_stats(server: &TestServer) -> ExternalControllerStatus {
        server
            .get("/stats")
            .send()
            .await
            .unwrap()
            .json::<ExternalControllerStatus>()
            .await
            .unwrap()
    }

    pub(super) async fn get_completion_status(
        server: &TestServer,
        token: &str,
    ) -> CompletionStatusResponse {
        server
            .get(format!("/completion_status?token={token}"))
            .send()
            .await
            .unwrap()
            .json::<CompletionStatusResponse>()
            .await
            .unwrap()
    }

    pub(super) async fn print_stats(server: &TestServer) {
        let stats = serde_json::to_string_pretty(&get_stats(server).await).unwrap();

        println!("{stats}")
    }

    pub(super) async fn start_test_server(config_str: &str, deployment_id: Uuid) -> TestServer {
        start_test_server_with_options(
            config_str,
            deployment_id,
            BootstrapConfig::from(BootstrapPolicy::Allow),
            &[None],
        )
        .await
    }

    pub(super) async fn start_test_server_with_options(
        config_str: &str,
        deployment_id: Uuid,
        bootstrap_config: BootstrapConfig,
        persistent_output_ids: &'static [Option<&'static str>],
    ) -> TestServer {
        let mut config_file = NamedTempFile::new().unwrap();
        config_file.write_all(config_str.as_bytes()).unwrap();

        println!("Creating HTTP server");

        let state = WebData::new(ServerState::new(
            PipelinePhase::Initializing(InitializationState::Starting),
            String::default(),
            RuntimeDesiredStatus::Paused,
            bootstrap_config,
            deployment_id,
            None,
            None,
            None,
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
            bootstrap_policy: bootstrap_config.active_bootstrap_policy(),
            silent_bootstrap: bootstrap_config.silent_bootstrap,
            deployment_id,
            host_id: None,
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
                        persistent_output_ids,
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

        server
    }

    pub(super) fn flatten_and_sort_batches(data: &[Vec<TestStruct>]) -> Vec<TestStruct> {
        let mut records = data
            .iter()
            .flat_map(|batch| batch.iter().cloned())
            .collect::<Vec<_>>();
        records.sort();
        records
    }

    fn decode_chunk_records(chunk: &crate::transport::http::Chunk) -> Vec<TestStruct> {
        let Some(csv) = &chunk.text_data else {
            return Vec::new();
        };

        let mut reader = CsvReaderBuilder::new()
            .has_headers(false)
            .from_reader(csv.as_bytes());

        reader
            .deserialize::<(TestStruct, i32)>()
            .map(Result::unwrap)
            .map(|(record, weight)| {
                assert_eq!(weight, 1);
                record
            })
            .collect()
    }

    pub(super) async fn collect_output_chunks<S>(
        response: &mut S,
        num_records: usize,
    ) -> (Vec<crate::transport::http::Chunk>, Vec<TestStruct>)
    where
        S: Stream<Item = Result<web::Bytes, PayloadError>> + Unpin,
    {
        let mut chunks = Vec::new();
        let mut records = Vec::with_capacity(num_records);
        let mut data = Vec::new();

        while records.len() < num_records {
            let bytes = response.next().await.unwrap().unwrap();
            data.extend_from_slice(&bytes);

            if data.last() != Some(&b'\n') {
                continue;
            }

            for chunk in serde_json::Deserializer::from_slice(&data)
                .into_iter::<crate::transport::http::Chunk>()
            {
                let chunk = chunk.unwrap();
                let chunk_records = decode_chunk_records(&chunk);
                if chunk_records.is_empty() {
                    continue;
                }
                records.extend(chunk_records);
                chunks.push(chunk);
            }

            data.clear();
        }

        (chunks, records)
    }

    pub(super) async fn wait_for_completion(server: &TestServer, token: &str) {
        async_wait(
            || async {
                let CompletionStatusResponse { status, .. } =
                    get_completion_status(server, token).await;
                status == CompletionStatus::Complete
            },
            20_000,
        )
        .await
        .unwrap();
    }

    pub(super) fn test_batches(start: u32, len: u32) -> Vec<Vec<TestStruct>> {
        vec![(start..start + len).map(TestStruct::for_id).collect()]
    }

    pub(super) fn batch_num_records(batches: &[Vec<TestStruct>]) -> u64 {
        batches.iter().map(|batch| batch.len() as u64).sum()
    }

    pub(super) fn read_file_output(output_path: &Path) -> Vec<TestStruct> {
        if !output_path.exists() {
            return Vec::new();
        }

        let mut actual = CsvReaderBuilder::new()
            .has_headers(false)
            .from_reader(File::open(output_path).unwrap())
            .deserialize::<(TestStruct, i32)>()
            .map(|res| {
                let (record, weight) = res.unwrap();
                assert_eq!(weight, 1);
                record
            })
            .collect::<Vec<_>>();
        actual.sort();
        actual
    }

    pub(super) async fn wait_for_status(server: &TestServer, expected: RuntimeStatus) {
        async_wait(
            || async {
                let Ok(status) = server
                    .get("/status")
                    .send()
                    .await
                    .unwrap()
                    .json::<ExtendedRuntimeStatus>()
                    .await
                else {
                    return false;
                };
                status.runtime_status == expected
            },
            20_000,
        )
        .await
        .unwrap();
    }

    pub(super) async fn start_pipeline(server: &TestServer) {
        println!("/start");
        let resp = server.get("/start").send().await.unwrap();
        assert!(resp.status().is_success());
        wait_for_status(server, RuntimeStatus::Running).await;
    }

    pub(super) async fn pause_pipeline(server: &TestServer) {
        println!("/pause");
        let resp = server.get("/pause").send().await.unwrap();
        assert!(resp.status().is_success());
        wait_for_status(server, RuntimeStatus::Paused).await;
    }

    /// Wait until the storage directory lock (`feldera.pidlock`) can be acquired,
    /// indicating that the previous pipeline runtime has released storage.
    pub(super) async fn wait_for_storage_unlock(storage_dir: &Path) {
        let storage_dir = storage_dir.to_path_buf();
        async_wait(
            || {
                let storage_dir = storage_dir.clone();
                async move {
                    tokio::task::spawn_blocking(move || {
                        // Probe whether the lock is free; release immediately if so.
                        LockedDirectory::new(&storage_dir).is_ok()
                    })
                    .await
                    .unwrap()
                }
            },
            20_000,
        )
        .await
        .unwrap();
    }

    /// Suspend the pipeline. When `storage_dir` is set, also wait until the
    /// previous runtime releases the storage lock before returning, so a new
    /// server can safely reopen the same storage directory.
    pub(super) async fn suspend_pipeline(server: &TestServer, storage_dir: Option<&Path>) {
        println!("/suspend");
        let resp = server.post("/suspend").send().await.unwrap();
        assert!(resp.status().is_success());
        wait_for_status(server, RuntimeStatus::Suspended).await;
        if let Some(storage_dir) = storage_dir {
            wait_for_storage_unlock(storage_dir).await;
        }
    }

    pub(super) async fn start_transaction(server: &TestServer) {
        println!("/start_transaction");
        let resp = server.post("/start_transaction").send().await.unwrap();
        assert!(resp.status().is_success());
    }

    pub(super) async fn commit_transaction(server: &TestServer) {
        println!("/commit_transaction");
        let resp = server.post("/commit_transaction").send().await.unwrap();
        assert!(resp.status().is_success());
    }

    pub(super) async fn start_input_endpoint(server: &TestServer, endpoint_name: &str) {
        println!("/input_endpoints/{endpoint_name}/start");
        let resp = server
            .get(format!("/input_endpoints/{endpoint_name}/start"))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());
    }

    pub(super) async fn pause_input_endpoint(server: &TestServer, endpoint_name: &str) {
        println!("/input_endpoints/{endpoint_name}/pause");
        let resp = server
            .get(format!("/input_endpoints/{endpoint_name}/pause"))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());
    }

    pub(super) async fn send_input_no_wait(
        server: &TestServer,
        data: &[Vec<TestStruct>],
    ) -> String {
        let CompletionTokenResponse { token } = TestHttpSender::send_stream_deserialize_resp::<
            CompletionTokenResponse,
        >(server.post("/ingress/test_input1"), data)
        .await;
        token
    }

    pub(super) async fn send_input(server: &TestServer, data: &[Vec<TestStruct>]) {
        let token = send_input_no_wait(server, data).await;
        wait_for_completion(server, &token).await;
    }

    pub(super) async fn wait_for_file_output(output_path: &Path, expected: &[Vec<TestStruct>]) {
        let mut expected = expected
            .iter()
            .flat_map(|batch| batch.iter())
            .cloned()
            .collect::<Vec<_>>();
        expected.sort();

        async_wait(
            || std::future::ready(read_file_output(output_path) == expected),
            20_000,
        )
        .await
        .unwrap();
    }

    fn output_metrics(stats: &ExternalControllerStatus) -> &ExternalOutputEndpointMetrics {
        stats
            .outputs
            .iter()
            .find(|output| output.endpoint_name == "test_output1")
            .map(|output| &output.metrics)
            .expect("test_output1 output endpoint")
    }

    /// After a silent-bootstrap restart, verify that backfilled records were
    /// processed but not written to the output file.
    ///
    /// Waits for bootstrap to finish and for the output connector to catch up
    /// with the checkpointed input count before asserting.
    pub(super) async fn assert_no_file_output(
        server: &TestServer,
        output_path: &Path,
        min_processed_records: u64,
        expected_transmitted_records: u64,
    ) {
        wait_for_status(server, RuntimeStatus::Paused).await;

        async_wait(
            || async {
                output_metrics(&get_stats(server).await).total_processed_input_records
                    >= min_processed_records
            },
            20_000,
        )
        .await
        .unwrap();

        let stats = get_stats(server).await;
        let metrics = output_metrics(&stats);
        assert_eq!(
            metrics.transmitted_records,
            expected_transmitted_records,
            "silent bootstrap must not transmit backfilled records, stats: {stats:#?}\nfile contents: {:?}",
            read_file_output(output_path)
        );
        assert_eq!(read_file_output(output_path), Vec::<TestStruct>::new());
    }
}

/// Server tests that exercise HTTP egress/ingress only — no Kafka required.
#[cfg(test)]
mod test_http {
    use super::test_http_helpers::{
        collect_output_chunks, flatten_and_sort_batches, get_completion_status, get_stats,
        print_stats, start_test_server, wait_for_completion,
    };
    use crate::{
        ensure_default_crypto_provider,
        server::test_http_helpers::{
            assert_no_file_output, batch_num_records, commit_transaction, pause_pipeline,
            send_input, send_input_no_wait, start_pipeline, start_test_server_with_options,
            start_transaction, suspend_pipeline, test_batches, wait_for_file_output,
        },
        test::{
            TestStruct, async_wait, generate_test_batches,
            http::{TestHttpReceiver, TestHttpSender},
        },
    };
    use feldera_types::{
        adapter_stats::TransactionStatus,
        completion_token::{CompletionStatus, CompletionStatusResponse, CompletionTokenResponse},
        runtime_status::{BootstrapConfig, BootstrapPolicy},
    };
    use proptest::{
        strategy::{Strategy, ValueTree},
        test_runner::TestRunner,
    };
    use std::{thread::sleep, time::Duration};
    use tempfile::TempDir;
    use tokio::time::timeout;
    use uuid::Uuid;

    /// Verifies the `send_snapshot` HTTP egress mode:
    /// 1. Pushes initial data, connects with `send_snapshot=true`, and
    ///    verifies the snapshot chunks carry `snapshot: true`.
    /// 2. Pushes more data and verifies incremental delta chunks arrive
    ///    with `snapshot: false`.
    /// 3. Opens a second connection and verifies it receives a full
    ///    snapshot of all data (initial + follow).
    #[actix_web::test]
    async fn test_http_egress_send_snapshot() {
        ensure_default_crypto_provider();

        let initial_data = vec![
            vec![TestStruct::for_id(1), TestStruct::for_id(2)],
            vec![TestStruct::for_id(3)],
        ];
        let follow_data = vec![vec![TestStruct::for_id(10), TestStruct::for_id(11)]];

        let server = start_test_server(
            r#"
name: test
inputs:
outputs:
"#,
            Uuid::new_v4(),
        )
        .await;

        start_pipeline(&server).await;

        let CompletionTokenResponse { token } =
            TestHttpSender::send_stream_deserialize_resp::<CompletionTokenResponse>(
                server.post("/ingress/test_input1"),
                &initial_data,
            )
            .await;
        wait_for_completion(&server, &token).await;

        let mut response = server
            .post("/egress/test_output1?format=csv&backpressure=true&send_snapshot=true")
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());

        let (snapshot_chunks, mut snapshot_records) = match timeout(
            Duration::from_secs(10),
            collect_output_chunks(&mut response, initial_data.iter().map(Vec::len).sum()),
        )
        .await
        {
            Ok(result) => result,
            Err(error) => {
                print_stats(&server).await;
                panic!("timed out waiting for snapshot output: {error:?}");
            }
        };
        snapshot_records.sort();
        assert_eq!(snapshot_records, flatten_and_sort_batches(&initial_data));
        assert!(!snapshot_chunks.is_empty());
        assert!(snapshot_chunks.iter().all(|chunk| chunk.snapshot));

        let CompletionTokenResponse { token } = TestHttpSender::send_stream_deserialize_resp::<
            CompletionTokenResponse,
        >(
            server.post("/ingress/test_input1"), &follow_data
        )
        .await;
        wait_for_completion(&server, &token).await;

        let (delta_chunks, mut delta_records) = timeout(
            Duration::from_secs(10),
            collect_output_chunks(&mut response, follow_data.iter().map(Vec::len).sum()),
        )
        .await
        .expect("timed out waiting for follow output");
        delta_records.sort();
        assert_eq!(delta_records, flatten_and_sort_batches(&follow_data));
        assert!(!delta_chunks.is_empty());
        assert!(delta_chunks.iter().all(|chunk| !chunk.snapshot));

        // Open a second connection: it should receive a snapshot of all data
        // (initial + follow) since the view is materialized.
        let all_data_len: usize = initial_data.iter().map(Vec::len).sum::<usize>()
            + follow_data.iter().map(Vec::len).sum::<usize>();

        let mut response2 = server
            .post("/egress/test_output1?format=csv&backpressure=true&send_snapshot=true")
            .send()
            .await
            .unwrap();
        assert!(response2.status().is_success());

        let (snapshot2_chunks, mut snapshot2_records) = timeout(
            Duration::from_secs(10),
            collect_output_chunks(&mut response2, all_data_len),
        )
        .await
        .expect("timed out waiting for second connection snapshot");
        snapshot2_records.sort();

        let mut all_expected = flatten_and_sort_batches(&initial_data);
        all_expected.extend(flatten_and_sort_batches(&follow_data));
        all_expected.sort();

        assert_eq!(snapshot2_records, all_expected);
        assert!(!snapshot2_chunks.is_empty());
        assert!(snapshot2_chunks.iter().all(|chunk| chunk.snapshot));
    }

    /// Connecting to `/egress?send_snapshot=true` while the pipeline is
    /// Paused must still deliver the cached materialized-view snapshot.
    ///
    /// Without the fix, the snapshot stalls indefinitely: registration calls
    /// `request_snapshot_delivery`, which short-circuits when the pipeline is
    /// not `Running`, so no step fires and `push_output` never runs the
    /// `is_snapshot_pending` path. The HTTP client receives only the 3-second
    /// keepalive empties and times out.
    #[actix_web::test]
    async fn test_http_egress_send_snapshot_while_paused() {
        ensure_default_crypto_provider();

        let initial_data = vec![
            vec![TestStruct::for_id(1), TestStruct::for_id(2)],
            vec![TestStruct::for_id(3)],
        ];

        let server = start_test_server(
            r#"
name: test
inputs:
outputs:
"#,
            Uuid::new_v4(),
        )
        .await;

        // Start, push data, wait for it to land in the materialized view so
        // `trace_snapshots` is populated before we pause.
        start_pipeline(&server).await;

        let CompletionTokenResponse { token } =
            TestHttpSender::send_stream_deserialize_resp::<CompletionTokenResponse>(
                server.post("/ingress/test_input1"),
                &initial_data,
            )
            .await;
        wait_for_completion(&server, &token).await;

        // Pause; subsequent egress request must still see the snapshot.
        // `pause_pipeline` waits until the circuit thread actually stops
        // stepping, so this exercises the truly-paused path.
        pause_pipeline(&server).await;

        let mut response = server
            .post("/egress/test_output1?format=csv&backpressure=true&send_snapshot=true")
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());

        let (snapshot_chunks, mut snapshot_records) = match timeout(
            Duration::from_secs(10),
            collect_output_chunks(&mut response, initial_data.iter().map(Vec::len).sum()),
        )
        .await
        {
            Ok(result) => result,
            Err(error) => {
                print_stats(&server).await;
                panic!(
                    "timed out waiting for snapshot output while paused: {error:?} \
                     (expected the snapshot to be delivered without resuming)"
                );
            }
        };
        snapshot_records.sort();
        assert_eq!(snapshot_records, flatten_and_sort_batches(&initial_data));
        assert!(
            !snapshot_chunks.is_empty(),
            "expected at least one chunk with payload"
        );
        assert!(
            snapshot_chunks.iter().all(|chunk| chunk.snapshot),
            "expected all data chunks to be marked snapshot=true while paused, got: {:?}",
            snapshot_chunks
                .iter()
                .map(|c| c.snapshot)
                .collect::<Vec<_>>()
        );
    }

    #[actix_web::test]
    async fn test_silent_bootstrap() {
        ensure_default_crypto_provider();

        let tempdir = TempDir::new().unwrap();
        let storage_dir = tempdir.path().join("storage");
        let output_path = tempdir.path().join("output.csv");
        std::fs::create_dir(&storage_dir).unwrap();

        let config_str = format!(
            r#"
name: test
workers: 4
storage_config:
    path: "{}"
storage: true
clock_resolution_usecs:
inputs:
outputs:
    test_output1:
        stream: test_output1
        transport:
            name: file_output
            config:
                path: "{}"
        format:
            name: csv
            config: {{}}
"#,
            storage_dir.display(),
            output_path.display()
        );

        let first_batch = test_batches(0, 10);
        let second_batch = test_batches(10, 10);
        let third_batch = test_batches(20, 10);

        let server = start_test_server_with_options(
            &config_str,
            Uuid::new_v4(),
            BootstrapConfig::from(BootstrapPolicy::Allow),
            &[Some("v0")],
        )
        .await;
        start_pipeline(&server).await;
        send_input(&server, &first_batch).await;
        wait_for_file_output(&output_path, &first_batch).await;

        let stats = get_stats(&server).await;
        println!("stats before suspend: {stats:#?}");

        suspend_pipeline(&server, Some(&storage_dir)).await;
        drop(server);

        let server = start_test_server_with_options(
            &config_str,
            Uuid::new_v4(),
            BootstrapConfig::from(BootstrapPolicy::Allow).with_silent_bootstrap(true),
            &[Some("v1")],
        )
        .await;
        assert_no_file_output(
            &server,
            &output_path,
            batch_num_records(&first_batch),
            batch_num_records(&first_batch),
        )
        .await;
        start_pipeline(&server).await;
        send_input(&server, &second_batch).await;
        wait_for_file_output(&output_path, &second_batch).await;
        suspend_pipeline(&server, Some(&storage_dir)).await;
        drop(server);

        let server = start_test_server_with_options(
            &config_str,
            Uuid::new_v4(),
            BootstrapConfig::from(BootstrapPolicy::Allow).with_silent_bootstrap(true),
            &[Some("v2")],
        )
        .await;
        let cumulative_records = batch_num_records(&first_batch) + batch_num_records(&second_batch);
        assert_no_file_output(
            &server,
            &output_path,
            cumulative_records,
            cumulative_records,
        )
        .await;
        start_pipeline(&server).await;
        send_input(&server, &third_batch).await;
        wait_for_file_output(&output_path, &third_batch).await;
        suspend_pipeline(&server, Some(&storage_dir)).await;
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
        let server = start_test_server(config_str, Uuid::default()).await;

        // Start pipeline.
        start_pipeline(&server).await;

        start_transaction(&server).await;

        println!("Connecting to HTTP output endpoint");
        let mut egress_resp = server
            .post("/egress/test_output1?backpressure=true")
            .send()
            .await
            .unwrap();

        let token = send_input_no_wait(&server, &data).await;
        println!("data sent");

        commit_transaction(&server).await;

        // Receive output first, otherwise the token never gets marked as complete.
        TestHttpReceiver::wait_for_output_unordered(&mut egress_resp, &data).await;
        wait_for_completion(&server, &token).await;
    }

    /// Regression test for issue #6231.
    /// In a pipeline without output connectors, the number of completed records,
    /// completed steps, and the `pipeline_complete` flag are updated before the current transaction
    /// completes.
    #[actix_web::test]
    async fn test_transaction_completion_without_output_connectors() {
        ensure_default_crypto_provider();

        let data = vec![vec![
            TestStruct::for_id(1),
            TestStruct::for_id(2),
            TestStruct::for_id(3),
        ]];
        let input_records = data.iter().map(Vec::len).sum::<usize>() as u64;

        // Keep the controller free of configured output connectors.  This
        // exercises the no-output path in completion accounting instead of
        // waiting for any egress connector to report progress.
        let config_str = r#"
name: test
inputs:
outputs:
"#;
        let server = start_test_server(config_str, Uuid::default()).await;

        start_pipeline(&server).await;

        start_transaction(&server).await;

        // Push input through HTTP ingress while the transaction is open.  The
        // endpoint returns a completion token for exactly the records that this
        // request placed into the pipeline.
        let req = server.post("/ingress/test_input1");
        let CompletionTokenResponse { token } =
            TestHttpSender::send_stream_deserialize_resp::<CompletionTokenResponse>(req, &data)
                .await;
        println!("completion token: {token}");

        // Give the pipeline time to process the input inside the transaction.
        // Before commit, records must not be reported as completed.
        sleep(Duration::from_millis(5000));
        let stats = get_stats(&server).await;
        assert!(!stats.global_metrics.pipeline_complete);
        assert_eq!(stats.global_metrics.total_completed_records, 0);

        let CompletionStatusResponse { status, .. } = get_completion_status(&server, &token).await;
        assert_eq!(status, CompletionStatus::InProgress);

        commit_transaction(&server).await;

        // Wait for the transaction to commit.
        async_wait(
            || async {
                let stats = get_stats(&server).await;
                stats.global_metrics.transaction_status == TransactionStatus::NoTransaction
            },
            20_000,
        )
        .await
        .unwrap();

        async_wait(
            || async {
                let CompletionStatusResponse { status, .. } =
                    get_completion_status(&server, &token).await;
                status == CompletionStatus::Complete
            },
            20_000,
        )
        .await
        .unwrap();

        let stats = get_stats(&server).await;
        assert!(stats.global_metrics.pipeline_complete);
        assert_eq!(stats.global_metrics.total_completed_records, input_records,);
    }
}

#[cfg(test)]
#[cfg(feature = "with-kafka")]
mod test_with_kafka {
    use super::test_http_helpers::{
        get_completion_status, get_stats, print_stats, start_test_server,
    };
    use crate::{
        controller::MAX_API_CONNECTIONS,
        ensure_default_crypto_provider,
        server::test_http_helpers::{
            pause_input_endpoint, pause_pipeline, send_input_no_wait, start_input_endpoint,
            start_pipeline, wait_for_completion,
        },
        test::{
            async_wait, generate_test_batches,
            http::{TestHttpReceiver, TestHttpSender},
            kafka::{BufferConsumer, KafkaResources, TestProducer},
        },
    };
    use feldera_types::completion_token::{CompletionStatus, CompletionTokenResponse};
    use proptest::{
        strategy::{Strategy, ValueTree},
        test_runner::TestRunner,
    };
    use serde_json::json;
    use std::{thread::sleep, time::Duration};
    use uuid::Uuid;

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
        let server = start_test_server(config_str, Uuid::new_v4()).await;

        // Write data to Kafka.
        println!("Send test data");
        let producer = TestProducer::new();
        producer.send_to_topic(&data, "test_server_input_topic");

        sleep(Duration::from_millis(2000));
        assert!(buffer_consumer.is_empty());

        // Start pipeline.
        start_pipeline(&server).await;

        sleep(Duration::from_millis(3000));

        // Unpause input endpoint.
        start_input_endpoint(&server, "test_input1").await;

        // Wait for data.
        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        println!("/stats");
        get_stats(&server).await;

        println!("/metadata");
        let resp = server.get("/metadata").send().await.unwrap();
        assert!(resp.status().is_success());

        // Pause input endpoint.
        pause_input_endpoint(&server, "test_input1").await;

        // Pause pipeline.
        pause_pipeline(&server).await;
        sleep(Duration::from_millis(1000));

        // Send more data, receive none
        producer.send_to_topic(&data, "test_server_input_topic");
        sleep(Duration::from_millis(2000));
        assert_eq!(buffer_consumer.len(), 0);

        // Start pipeline; still no data because the endpoint is paused.
        start_pipeline(&server).await;

        sleep(Duration::from_millis(2000));
        assert_eq!(buffer_consumer.len(), 0);

        // Resume input endpoint, receive data.
        start_input_endpoint(&server, "test_input1").await;

        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        println!("Testing invalid input");
        producer.send_string("invalid\n", "test_server_input_topic");
        loop {
            let stats = get_stats(&server).await;
            // println!("stats: {stats:#}");
            let num_errors = stats.inputs[0].metrics.num_parse_errors;
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
            assert!(
                server
                    .post("/egress/test_output1")
                    .send()
                    .await
                    .unwrap()
                    .status()
                    .is_success()
            );
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
        let token = send_input_no_wait(&server, &data).await;
        println!("data sent");

        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        TestHttpReceiver::wait_for_output_unordered(&mut resp1, &data).await;
        TestHttpReceiver::wait_for_output_unordered(&mut resp2, &data).await;
        wait_for_completion(&server, &token).await;

        // Force-push data in paused state.
        pause_pipeline(&server).await;
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

                        let status = get_completion_status(&server, &token).await.status;
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

        start_pipeline(&server).await;

        sleep(Duration::from_millis(5000));

        pause_pipeline(&server).await;
        sleep(Duration::from_millis(1000));

        drop(buffer_consumer);
        drop(kafka_resources);
    }
}
