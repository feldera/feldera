use crate::adhoc::set_snapshot;
use crate::controller::{CompletionToken, ConsistentSnapshot, ControllerBuilder};
use crate::engine::{
    CircuitFactory, Engine, JsonProfileChunks, OpenStorageFn, PipelineStorage, ProgramLoader,
};
use crate::format::csv::CsvOutputFormat;
use crate::format::get_input_format;
use crate::format::json::JsonOutputFormat;
use crate::panic::enable_counting_panics;
use crate::server::metrics::{
    JsonFormatter, LabelStack, MetricsFormatter, MetricsWriter, PrometheusFormatter,
};
use crate::transport::http::HttpOutputFormat;
use crate::util::{
    LongOperationWarning, RateLimitCheckResult, TokenBucketRateLimiter,
    missing_pipeline_identity_message,
};
use crate::{
    Controller, ControllerError, FormatConfig, InputEndpointConfig, OutputEndpoint,
    OutputEndpointConfig, PipelineConfig, TransportInputEndpoint,
    adhoc::{adhoc_websocket, stream_adhoc_result},
    controller::ConnectorConfig,
    ensure_default_crypto_provider,
    samply::{SamplyProfile, SamplyState, SamplyStatus},
    transport::http::{
        HttpInputEndpoint, HttpInputTransport, HttpOutputEndpoint, HttpOutputTransport,
        RequestMetadata,
    },
};
use crate::{ControllerStatus, dyn_event};
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
    middleware::Compress,
    post, rt,
    web::{self, Data as WebData, Payload, Query},
};
use arrow::ipc::writer::StreamWriter;
use async_stream;
use bytes::Bytes;
use chrono::Utc;
use clap::Parser;
use colored::Colorize;
use feldera_adapterlib::PipelineState;
use feldera_adapterlib::errors::controller::ConfigError;
use feldera_observability as observability;
use feldera_observability::json_logging::init_pipeline_logging_with_id;
use feldera_types::adapter_stats::{
    EndpointErrorStats, InputEndpointErrorMetrics, OutputEndpointErrorMetrics,
    PipelineStatsErrorsResponse,
};
use feldera_types::checkpoint::{
    CheckpointFailure, CheckpointPullStatus, CheckpointResponse, CheckpointStatus,
    CheckpointStatusQuery, CheckpointSyncFailure, CheckpointSyncResponse, CheckpointSyncStatus,
    HostInfo,
};
use feldera_types::circuit::Layout;
use feldera_types::completion_token::{
    CompletionStatusArgs, CompletionStatusResponse, CompletionTokenResponse,
};
use feldera_types::config::{PipelineIdentity, SyncConfig};
use feldera_types::coordination::{
    AdHocScan, CoordinationActivate, CoordinationCapabilities, CoordinationStatus, InputChoices,
    Labels, RestartArgs, Step, StepRequest,
};
use feldera_types::format::json::JsonEncoderConfig;
use feldera_types::pipeline_diff::PipelineDiff;
use feldera_types::query_params::{
    ActivateParams, ApproveParameters, MetricsFormat, MetricsParameters, SamplyProfileGetParams,
    SamplyProfileParams, StatsParams,
};
use feldera_types::runtime_status::{
    BootstrapConfig, BootstrapPolicy, ConnectorStats, ExtendedRuntimeStatus,
    ExtendedRuntimeStatusError, RuntimeDesiredStatus, RuntimeStatus, RuntimeStatusDetails,
    StorageStatusDetails,
};
use feldera_types::size::ConfigSize;
use feldera_types::suspend::{SuspendError, SuspendableResponse};
use feldera_types::time_series::TimeSeries;
use feldera_types::transaction::ConcurrentBootstrapPhase;
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
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::convert::Infallible;
use std::hash::{BuildHasherDefault, DefaultHasher, Hash, Hasher};
use std::mem::take;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{MutexGuard, OnceLock};
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

    /// True if the pipeline never leaves this phase: it has failed or has been
    /// suspended, and either way it never runs again.
    fn is_terminal(&self) -> bool {
        match self {
            PipelinePhase::InitializationError(_)
            | PipelinePhase::Failed(_)
            | PipelinePhase::Suspended => true,
            PipelinePhase::Initializing(_) | PipelinePhase::InitializationComplete => false,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct CheckpointState {
    /// Sequence number for the next checkpoint request.
    next_seq: u64,

    /// The UUID of the last checkpoint.
    last_checkpoint: Option<Uuid>,

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

    /// Leaf lock (no more locks may be taken while holding it).
    ///
    /// Use `controller()`, `phase()` and the [Lifecycle] transitions to
    /// access.
    ///
    /// This lock is only held momentarily.
    lifecycle: Mutex<Lifecycle>,

    /// Leaf lock.
    checkpoint_state: Mutex<CheckpointState>,

    /// Leaf lock.
    sync_checkpoint_status: Mutex<CheckpointSyncStatus>,

    /// Leaf lock.
    /// Samply profiling state.
    samply_state: Arc<Mutex<SamplyState>>,

    /// Deployment ID.
    ///
    /// This comes from [ServerArgs].  It remains unchanged across automatic
    /// restarts.
    deployment_id: Uuid,

    /// Identity of this pipeline (system name + given name).
    ///
    /// Used by the `/coordination/checkpoint/pull` endpoint to identify the
    /// pipeline when checking S3 bucket ownership.  `None` when the pipeline has
    /// no system-assigned name.
    pipeline_identity: Option<PipelineIdentity>,

    /// Incarnation UUID.
    ///
    /// This is randomly generated each time the process starts.
    incarnation_uuid: Uuid,

    metadata: String,

    /// The pipeline's storage, if it has any.
    storage: Option<Arc<dyn PipelineStorage>>,

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

    /// Leaf lock.
    ///
    /// The values that this host's distributed input connectors chose for all
    /// the hosts to use, once the connectors have initialized.  This is set
    /// before initialization completes, because completing it needs the other
    /// hosts, which the coordinator activates only after it reads this.
    input_choices: Mutex<Option<BTreeMap<String, serde_json::Value>>>,

    /// Current collection of leases.
    leases: Mutex<HashMap<Step, Lease>>,

    /// What `POST /testing/load` needs to replace the program; set from
    /// `--enable-program-load`.
    program_load: OnceLock<ProgramLoad>,

    /// Counts the programs loaded into this process.  Per-program caches and the
    /// controller's error callback compare against it, so nothing from a
    /// replaced program reaches the current one.  Changes only while `lifecycle`
    /// is locked, together with the phase; readers that need the two to agree
    /// read it under that lock.
    program_generation: AtomicU64,
}

/// What `POST /testing/load` needs to replace the running program.
struct ProgramLoad {
    /// Builds the circuit of a program that a request sends.
    loader: Box<dyn ProgramLoader>,

    /// Opens the storage of a loaded program's controller.
    open_storage: OpenStorageFn,
}

/// Pipeline phase + controller.
///
/// `controller` and `phase` answer one question between them, so they are
/// guarded together: several threads publish them concurrently, and updating
/// them separately lets an observer see a pair that never legitimately occurs.
///
/// Every transition that changes both fields does so in one critical section
/// (see [`ServerState::complete_initialization`], [`ServerState::fail`] and
/// [`ServerState::suspended`]), so the pair is always consistent and
/// [`ServerState::lifecycle`] can read it without coordinating with the writers.
#[derive(Clone)]
struct Lifecycle {
    /// Tracks the health of the pipeline.
    phase: PipelinePhase,

    /// The controller, once initialization has produced one and before a
    /// failure or a suspend has deallocated it.
    controller: Option<Controller>,
}

impl Lifecycle {
    /// Records `phase`, unless a terminal phase was already recorded: the first
    /// terminal phase is final (see [`PipelinePhase::is_terminal`]), because a
    /// thread that reports a later phase is not necessarily better informed.
    ///
    /// Returns whether the phase was recorded.
    fn set_phase(&mut self, phase: PipelinePhase) -> bool {
        if self.phase.is_terminal() {
            return false;
        }
        self.phase = phase;
        true
    }
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
        pipeline_identity: Option<PipelineIdentity>,
        storage: Option<Arc<dyn PipelineStorage>>,
        sync_config: Option<SyncConfig>,
        host_info: Option<HostInfo>,
    ) -> Self {
        // Max 10 errors per minute
        let rate_limiter = TokenBucketRateLimiter::new(10, Duration::from_secs(60));
        Self {
            lifecycle: Mutex::new(Lifecycle {
                phase,
                controller: None,
            }),
            desired_status_change: Arc::default(),
            metadata: md,
            checkpoint_state: Default::default(),
            sync_checkpoint_status: Default::default(),
            desired_status: Mutex::new(desired_status),
            bootstrap_config: Mutex::new(bootstrap_config),
            deployment_id,
            pipeline_identity,
            storage,
            sync_config,
            host_info,
            pull_state: Default::default(),
            rate_limiter,
            samply_state: Default::default(),
            coordination_activate: Default::default(),
            input_choices: Default::default(),
            leases: Default::default(),
            incarnation_uuid: Uuid::now_v7(),
            program_load: OnceLock::new(),
            program_generation: AtomicU64::new(0),
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
            None,
        )
    }

    /// Generate an appropriate error when the controller is `None`, which can
    /// mean that the pipeline is initializing, failed to initialize, has been
    /// shut down or failed.
    ///
    /// Takes `phase` rather than reading it, because the callers hold the
    /// `runtime` lock and it is not reentrant.
    fn missing_controller_error(phase: &PipelinePhase) -> PipelineError {
        match phase {
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
        let lifecycle = self.lifecycle.lock().unwrap();
        lifecycle
            .controller
            .clone()
            .ok_or_else(|| Self::missing_controller_error(&lifecycle.phase))
    }

    /// Reads the phase and the controller together, so the caller sees a
    /// consistent pair.
    fn lifecycle(&self) -> Lifecycle {
        self.lifecycle.lock().unwrap().clone()
    }

    /// Publishes `controller` as the result of a successful initialization.
    ///
    /// Hands `controller` back without publishing it if the pipeline has already
    /// reached a terminal phase, which happens when the circuit thread reports a
    /// fatal error while `do_bootstrap` is still finishing: the circuit thread
    /// reports that initialization is complete and then starts stepping the
    /// circuit right away (see `CircuitThread::run`).  Publishing a controller
    /// that a failure has already invalidated would claim the pipeline is
    /// runnable when it is not, and the caller owns stopping the circuit that
    /// controller belongs to, because nothing else can reach it any more.
    fn complete_initialization(&self, controller: Controller) -> Result<(), Controller> {
        let published = {
            let mut lifecycle = self.lifecycle.lock().unwrap();
            if lifecycle.set_phase(PipelinePhase::InitializationComplete) {
                lifecycle.controller = Some(controller);
                Ok(())
            } else {
                Err(controller)
            }
        };
        if published.is_ok() {
            self.desired_status_change.notify_waiters();
        }
        published
    }

    /// Records that the pipeline running program `generation` failed with
    /// `error`, and returns the controller to stop if one was still allocated.
    ///
    /// Recording the failure and deallocating the controller in one step keeps
    /// `/status` from ever seeing a failed pipeline that still advertises a
    /// controller, or a controller-less pipeline that still advertises
    /// [`PipelinePhase::InitializationComplete`].
    ///
    /// An error from a program that `/testing/load` has replaced changes
    /// nothing.  The generation is checked under the `lifecycle` lock, which
    /// [`ServerState::reset_for_program_load`] also holds while it moves to the
    /// next program, so a late error cannot fail its replacement.
    fn fail(&self, generation: u64, error: Arc<ControllerError>) -> Option<Controller> {
        let (recorded, controller) = {
            let mut lifecycle = self.lifecycle.lock().unwrap();
            if self.program_generation() != generation {
                return None;
            }
            let recorded = lifecycle.set_phase(PipelinePhase::Failed(error));
            (recorded, lifecycle.controller.take())
        };
        if recorded {
            self.desired_status_change.notify_waiters();
        }
        controller
    }

    /// Records the outcome of a suspend, and returns the controller to stop if
    /// one was still allocated.
    ///
    /// `error` is the suspend's own failure, if it had one.  A pipeline that
    /// already failed keeps that failure: a suspend gives up waiting for a
    /// controller as soon as the phase turns terminal, so it arrives here with
    /// nothing of its own to report even though no checkpoint was written.
    fn suspended(&self, error: Option<Arc<ControllerError>>) -> Option<Controller> {
        let phase = match error {
            Some(error) => PipelinePhase::Failed(error),
            None => PipelinePhase::Suspended,
        };
        let (recorded, controller) = {
            let mut lifecycle = self.lifecycle.lock().unwrap();
            let recorded = lifecycle.set_phase(phase);
            (recorded, lifecycle.controller.take())
        };
        if recorded {
            self.desired_status_change.notify_waiters();
        }
        controller
    }

    /// Deallocates the controller without recording why, so that a test can
    /// simulate a pipeline process dying without reporting anything.
    ///
    /// Production code deallocates the controller only as part of recording a
    /// terminal phase, which is what keeps the two consistent; see
    /// [`Lifecycle`].
    #[cfg(any(test, feature = "testing"))]
    fn abandon_controller(&self) -> Option<Controller> {
        self.lifecycle.lock().unwrap().controller.take()
    }

    /// Lets `POST /testing/load` replace the program, with programs that `loader`
    /// builds and controllers that open their storage with `open_storage`.
    /// Only the first call has an effect.
    fn enable_program_load(&self, loader: Box<dyn ProgramLoader>, open_storage: OpenStorageFn) {
        let _ = self.program_load.set(ProgramLoad {
            loader,
            open_storage,
        });
    }

    /// Discards the current program so that `/testing/load` can bootstrap
    /// another one, and returns the controller to stop if one was allocated.
    ///
    /// This is the only transition that leaves a terminal phase: loading a new
    /// program starts a new pipeline in the same process.  It refuses while an
    /// initialization is in flight, because that bootstrap thread would publish
    /// its controller over the new program.
    fn reset_for_program_load(&self) -> Result<Option<Controller>, PipelineError> {
        let mut desired_status = self.desired_status.lock().unwrap();
        match *desired_status {
            RuntimeDesiredStatus::Running | RuntimeDesiredStatus::Paused => (),
            other => return Err(PipelineError::InvalidTransition("load a program", other)),
        }
        let controller = {
            let mut lifecycle = self.lifecycle.lock().unwrap();
            if matches!(lifecycle.phase, PipelinePhase::Initializing(_)) {
                return Err(PipelineError::Initializing);
            }
            lifecycle.phase = PipelinePhase::Initializing(InitializationState::Starting);
            // Under the same lock as the phase change: `fail` checks the
            // generation while holding it.
            self.program_generation.fetch_add(1, Ordering::AcqRel);
            lifecycle.controller.take()
        };
        *desired_status = RuntimeDesiredStatus::Running;
        drop(desired_status);
        self.desired_status_change.notify_waiters();
        Ok(controller)
    }

    /// The program that this process runs: 0 for the first, one more for each
    /// program that `/testing/load` loads.
    fn program_generation(&self) -> u64 {
        self.program_generation.load(Ordering::Acquire)
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
        self.lifecycle.lock().unwrap().phase.clone()
    }

    /// Publishes the values that this host's distributed input connectors
    /// chose for all the hosts to use (see `/coordination/input/choices`).
    pub fn set_input_choices(&self, choices: BTreeMap<String, serde_json::Value>) {
        *self.input_choices.lock().unwrap() = Some(choices);
    }

    /// Records `phase` and leaves the controller as it is.
    ///
    /// Use this for the phases that report progress through initialization, and
    /// for an initialization failure: none of them allocate or deallocate the
    /// controller.  The transitions that do are
    /// [`Self::complete_initialization`], [`Self::fail`] and
    /// [`Self::suspended`], which update the phase and the controller together.
    ///
    /// See [`Lifecycle::set_phase`] for what happens if a terminal phase was
    /// already recorded.
    pub fn set_phase(&self, phase: PipelinePhase) {
        // Bind the result rather than testing it inline: a temporary lock guard in
        // an `if` condition lives until the end of the `if`, and `lifecycle` is a
        // leaf lock, so the notification below must not run while it is held.
        let recorded = self.lifecycle.lock().unwrap().set_phase(phase);
        if recorded {
            self.desired_status_change.notify_waiters();
        }
    }

    fn sync_checkpoint_status(&self) -> MutexGuard<'_, CheckpointSyncStatus> {
        self.sync_checkpoint_status.lock().unwrap()
    }
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct ServerArgs {
    /// Pipeline configuration JSON file.
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

    /// Bootstrap new and modified views concurrently, keeping pre-existing
    /// views live while the new ones backfill. Mutually exclusive with
    /// `--silent-bootstrap`.
    #[arg(long, action = clap::ArgAction::SetTrue)]
    pub concurrent_bootstrap: bool,

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

    /// Serve `POST /testing/load`, which replaces the running program without
    /// restarting the process, so a test harness can run many programs in one
    /// process.  Only pipelines without storage, fault tolerance, or multihost
    /// can be loaded this way.
    #[arg(long, action = clap::ArgAction::SetTrue)]
    pub enable_program_load: bool,
}

/// Returns the circuit factory of the pipeline that a configuration describes.
pub type MakeCircuitFactory =
    Box<dyn FnOnce(&PipelineConfig) -> Result<CircuitFactory, ControllerError> + Send + 'static>;

/// The server's command line: its own arguments and the engine's.
#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct EngineServerArgs<A: clap::Args> {
    #[command(flatten)]
    server: ServerArgs,

    #[command(flatten)]
    engine: A,
}

/// Server main function of a pipeline that `engine` runs.
///
/// Parses the command line, with the engine's arguments, and runs the server.
///
/// # Arguments
///
/// * `engine` - the engine that runs the pipeline's circuit.
///
/// # Returns
///
/// The error that stopped the server, if any, which this function also logs.
pub fn server_main<E: Engine>(engine: E) -> Result<(), ControllerError> {
    let args = EngineServerArgs::<E::Args>::try_parse()
        .map_err(|e| ControllerError::cli_args_error(&e))?;
    let engine = Arc::new(engine);
    let open_storage: OpenStorageFn = {
        let engine = engine.clone();
        Arc::new(move |config| engine.open_storage(config))
    };
    let engine_args = args.engine;
    let program_loader = engine.program_loader(&engine_args);
    let make_factory: MakeCircuitFactory =
        Box::new(move |config| engine.circuit_factory(config, &engine_args));

    run_server(args.server, make_factory, open_storage, program_loader).map_err(|e| {
        error!("{e}");
        e
    })
}

// We pass the engine as trait objects to make sure that this function
// doesn't have any generics and gets generated, along with the entire web server
// as part of the adapters crate. This reduces the size of the generated code for
// the main crate by ~300L lines of LLVM code.
pub fn run_server(
    args: ServerArgs,
    make_factory: MakeCircuitFactory,
    open_storage: OpenStorageFn,
    program_loader: Option<Box<dyn ProgramLoader>>,
) -> Result<(), ControllerError> {
    check_program_load_flag(&args, program_loader.is_some())?;
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
            "Pipeline tracing was enabled but the 'tracing' option is deprecated and has no effect."
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
        make_factory: MakeCircuitFactory,
        open_storage: OpenStorageFn,
        runtime: &actix_web::rt::Runtime,
    ) -> Result<WebData<ServerState>, ControllerError> {
        #[cfg(not(feature = "feldera-enterprise"))]
        if config.global.fault_tolerance.is_enabled() {
            return Err(ControllerError::EnterpriseFeature("fault tolerance"));
        }

        let circuit_factory = make_factory(config)?;

        // Initiate creating the controller so that we can get access to storage,
        // which is needed to determine the initial state.
        let builder = ControllerBuilder::new(config, &open_storage)?;
        info!(
            "Pipeline deployment identifier from arguments: {}",
            args.deployment_id
        );
        info!("Desired status from arguments: {:?}", args.initial);
        let (initial_status, bootstrap_config) = match builder
            .storage()
            .and_then(|storage| storage.read_status())
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
                        .with_silent_bootstrap(args.silent_bootstrap)
                        .with_concurrent_bootstrap(args.concurrent_bootstrap),
                )
            }
            Some(stored) if stored.deployment_id == args.deployment_id => {
                // This is an automatic restart (otherwise the deployment ID would
                // have changed).  Use the stored desired status.
                (
                    stored.desired_status,
                    BootstrapConfig::from(stored.bootstrap_policy)
                        .with_silent_bootstrap(stored.silent_bootstrap)
                        .with_concurrent_bootstrap(stored.concurrent_bootstrap),
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
                        .with_silent_bootstrap(args.silent_bootstrap)
                        .with_concurrent_bootstrap(args.concurrent_bootstrap),
                )
            }
            None => {
                // Initial deployment of this pipeline.  Use the desired status
                // passed in by the pipeline manager (it's the only one we have,
                // anyway).
                (
                    args.initial,
                    BootstrapConfig::from(args.bootstrap_policy)
                        .with_silent_bootstrap(args.silent_bootstrap)
                        .with_concurrent_bootstrap(args.concurrent_bootstrap),
                )
            }
        };

        // `silent_bootstrap` and `concurrent_bootstrap` are mutually exclusive.
        bootstrap_config
            .validate()
            .map_err(|error| ControllerError::BootstrapNotAllowed { error })?;

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
            config.pipeline_identity(),
            builder.storage(),
            builder.sync_config(),
            host_info,
        ));

        // Initialize the pipeline in a separate thread.  On success, this thread
        // will create a `Controller` instance and store it in `state.controller`.
        let storage = builder.storage();
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
                        concurrent_bootstrap: bootstrap_config.concurrent_bootstrap,
                        deployment_id: state.deployment_id,
                    };
                    if Some(stored_status) != prev_stored_status {
                        let storage = storage.clone();
                        let _ = spawn_blocking(move || storage.write_status(&stored_status)).await;
                    }
                    prev_stored_status = Some(stored_status);
                    notify.await;
                }
            });
        }

        Ok(state)
    }

    let system_runner = rt::System::new();

    let state = start_controller(
        &args,
        &config,
        make_factory,
        open_storage.clone(),
        system_runner.runtime(),
    )
    .unwrap_or_else(|error| {
        error!("Initialization failed: {error}");
        WebData::new(ServerState::for_error(error, args.deployment_id))
    });
    // Also on a failed start, so a harness can load the next program.
    if let Some(loader) = program_loader
        && args.enable_program_load
    {
        state.enable_program_load(loader, open_storage);
    }

    let workers = if let Some(http_workers) = config.global.http_workers {
        http_workers as usize
    } else {
        config.global.workers as usize
    };

    #[cfg(unix)]
    let sigterm_state = state.clone();

    let server = HttpServer::new({
        move || {
            let state = state.clone();
            let app = App::new().wrap_fn(|req, srv| {
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
            });
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

    // Otherwise actix stops the server on SIGTERM before the checkpoint.
    #[cfg(unix)]
    let server = server.disable_signals();

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

        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let sigterm = signal(SignalKind::terminate())
                .map_err(|e| ControllerError::io_error("listening for SIGTERM", e))?;
            spawn(checkpoint_on_sigterm(sigterm, sigterm_state));
        }

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

/// Writes a checkpoint on SIGTERM, then exits by raising SIGTERM again.
#[cfg(unix)]
async fn checkpoint_on_sigterm(
    mut sigterm: tokio::signal::unix::Signal,
    state: WebData<ServerState>,
) {
    use signal_hook::{consts::SIGTERM, low_level::emulate_default_handler};

    sigterm.recv().await;
    info!("received SIGTERM");
    match checkpoint_before_exit(&state).await {
        Ok(true) => info!("checkpoint written, exiting"),
        Ok(false) => info!(
            "pipeline has no circuit, no fault tolerance, or more than one host, exiting without a checkpoint"
        ),
        Err(error) => error!("checkpoint failed ({error}), exiting"),
    }
    // Does not return: if raising SIGTERM fails, it aborts.
    let _ = emulate_default_handler(SIGTERM);
}

/// Writes a checkpoint.
///
/// # Returns
/// `false` if there is no circuit, if fault tolerance is disabled (Force Stop
/// also sends SIGTERM), or if the pipeline is multihost (only the coordinator
/// can checkpoint all hosts at once).
async fn checkpoint_before_exit(state: &ServerState) -> Result<bool, Arc<ControllerError>> {
    let Ok(controller) = state.controller() else {
        return Ok(false);
    };
    let ft = &controller.status().pipeline_config.global.fault_tolerance;
    if !ft.is_enabled() || controller.layout().is_multihost() {
        return Ok(false);
    }
    controller.async_checkpoint().await.map(|_| true)
}

/// Refuses `--enable-program-load` for an engine that cannot load a program at
/// run time.
///
/// # Arguments
///
/// * `args` - the server's command line.
/// * `can_load` - whether the engine has a program loader.
fn check_program_load_flag(args: &ServerArgs, can_load: bool) -> Result<(), ControllerError> {
    if args.enable_program_load && !can_load {
        return Err(ControllerError::cli_args_error(
            &"This pipeline's engine cannot load a program at run time, so it does not accept \
              '--enable-program-load'.",
        ));
    }
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

    let path = config_file.as_ref();
    let mut config: PipelineConfig = serde_json::from_str(&read_config_file_as_string(path)?)
        .map_err(|e| {
            ControllerError::pipeline_config_parse_error(&format!(
                "configuration file '{}' must be JSON (YAML is no longer read): {e}",
                path.display()
            ))
        })?;

    // This is run in the pipeline process itself, so on a multihost deployment every
    // host derives the limit from its own machine.
    if config.global.effective_memory().is_none()
        && let Some(available_mb) = observability::system::total_memory_megabyte()
    {
        let available = ConfigSize::from_mb(available_mb);
        // The logger is not running yet.
        eprintln!(
            "No memory limit configured ('max_rss' or 'resources.memory_max'); \
using this host's available memory: 'max_rss' = {available}."
        );
        config.global.max_rss = Some(available);
    }

    eprintln!(
        "Pipeline configuration loaded successfully: {}",
        config.display_summary()
    );

    Ok(config)
}

// Initialization thread function.
fn bootstrap(
    builder: ControllerBuilder,
    circuit_factory: CircuitFactory,
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
        ControllerError::DbspError { .. }
            | ControllerError::DbspPanic
            | ControllerError::ControllerPanic
    )
}

/// Handle errors from the controller.
///
/// `generation` is the program the reporting controller runs; an error from a
/// program that `/testing/load` has replaced must not fail the current one.
fn error_handler(
    state: &Weak<ServerState>,
    generation: u64,
    error: Arc<ControllerError>,
    tag: Option<String>,
) {
    let state = match state.upgrade() {
        None => {
            // if we fail to upgrade state,
            // log the error anyways without checking for rate limiter
            error!("{error}");
            return;
        }
        Some(state) => state,
    };
    // Keeps a replaced program's errors out of the log; `fail` makes the
    // decision that counts, under the lock a program load also holds.
    if state.program_generation() != generation {
        info!("Ignoring an error from a replaced program: {error}");
        return;
    }

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
        && let Some(controller) = state.fail(generation, error)
    {
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
    circuit_factory: CircuitFactory,
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
                    builder = builder.with_output_assignment(take(&mut ca.output_assignment));

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
                    builder = builder.with_input_distribution(
                        take(&mut ca.input_distribution),
                        take(&mut ca.input_choices),
                    );
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
            let generation = state.program_generation();
            move |e, t| error_handler(&weak_state_ref, generation, e, t)
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
    // Publish under `desired_status` so that `/start` and `/pause` either run
    // before this and are picked up by the `match` above, or run after and find
    // the controller.
    let published = state.complete_initialization(controller);
    drop(desired_status);

    match published {
        Ok(()) => info!("Pipeline initialization complete"),
        Err(controller) => {
            // The failure that refused publication was recorded before the
            // controller existed, so `fail` had no controller to stop. A circuit
            // whose step fails keeps stepping (see `CircuitThread::step_circuit`),
            // so stop it here: this is the last reference to it, and without this
            // it runs on, holding the storage lock, after the pipeline has been
            // reported dead.
            warn!("Pipeline failed while initialization was being completed");
            controller.initiate_stop();
        }
    }

    Ok(())
}

fn build_app<T>(app: App<T>, state: WebData<ServerState>) -> App<T>
where
    T: ServiceFactory<ServiceRequest, Config = (), Error = ActixError, InitError = ()>,
{
    let program_load_enabled = state.program_load.get().is_some();
    let app = app
        .app_data(state)
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
        // Compressed when the client's `Accept-Encoding` admits it. `Compress` is
        // only used for profiles; the other live streams (`/egress`,
        // `/ingress`, `/time_series_stream`) are uncompressed, because gzip
        // frame buffering stalls their readers (see PR #5626).
        .service(
            web::resource("/dump_json_profile")
                .wrap(Compress::default())
                .route(web::get().to(dump_json_profile)),
        )
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
        .service(pause_output_endpoint)
        .service(start_output_endpoint)
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
        .service(coordination_input_choices)
        .service(coordination_restart)
        .service(clock_advance);
    if program_load_enabled {
        app.service(load_program)
    } else {
        app
    }
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
    // The values passed to `/approve` are definitive: `silent_bootstrap` and
    // `concurrent_bootstrap` are taken straight from the request (not OR-ed with
    // the startup configuration), so the two flags are handled symmetrically.
    let bootstrap_config = BootstrapConfig::from(BootstrapPolicy::Allow)
        .with_silent_bootstrap(args.silent_bootstrap)
        .with_concurrent_bootstrap(args.concurrent_bootstrap);
    bootstrap_config
        .validate()
        .map_err(|error| PipelineError::InvalidParam { error })?;
    state.set_bootstrap_config(bootstrap_config);

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
#[allow(clippy::result_large_err)]
async fn status_handler(
    state: WebData<ServerState>,
) -> Result<ExtendedRuntimeStatus, ExtendedRuntimeStatusError> {
    get_status(&state, true)
}

#[allow(clippy::result_large_err)]
/// Reports the runtime status of a controller whose circuit has reached
/// [`PipelineState::Terminated`].
///
/// A successful suspend terminates the circuit before the server installs
/// [`PipelinePhase::Suspended`] and deallocates the controller (see `/suspend`),
/// so a status poll landing in that window observes the still-registered
/// controller in `Terminated`. Report the clean `Suspended` status the user asked
/// for rather than a spurious `PipelineTerminated` error that the pipeline
/// manager would record as a failed execution.
///
/// `suspended` must say that a suspend is what terminated the circuit (see
/// `GlobalControllerMetrics::suspended`), not merely that one was requested. A
/// circuit that dies while a suspend is pending also lands here, and reporting
/// that as a clean `Suspended` would have the pipeline manager record a
/// successful suspend for a pipeline that wrote no checkpoint, and later resume
/// from a checkpoint that does not exist.
fn terminated_status(
    suspended: bool,
    runtime_desired_status: RuntimeDesiredStatus,
    storage_status_details: Option<StorageStatusDetails>,
) -> Result<ExtendedRuntimeStatus, ExtendedRuntimeStatusError> {
    if suspended {
        Ok(ExtendedRuntimeStatus {
            runtime_status: RuntimeStatus::Suspended,
            runtime_status_details: json!(""),
            runtime_desired_status,
            storage_status_details,
        })
    } else {
        Err(ExtendedRuntimeStatusError {
            status_code: StatusCode::INTERNAL_SERVER_ERROR,
            error: feldera_types::error::ErrorResponse {
                message: "Pipeline has been terminated.".to_string(),
                error_code: Cow::from("PipelineTerminated"),
                details: json!({}),
            },
        })
    }
}

#[allow(clippy::result_large_err)]
fn get_status(
    state: &ServerState,
    with_storage_status_details: bool,
) -> Result<ExtendedRuntimeStatus, ExtendedRuntimeStatusError> {
    // Runtime desired status
    let runtime_desired_status = state.desired_status();

    // Storage status details
    let storage_status_details = if with_storage_status_details
        && let Some(storage) = &state.storage
    {
        storage.checkpoints().inspect_err(|e| error!(
                    "Unable to read checkpoints; storage status details are not provided. Error: {e}"
                )).ok().map(|list_checkpoints| StorageStatusDetails {
                checkpoints: list_checkpoints.into(),
            })
    } else {
        None
    };

    // Current status.  Read the phase and the controller as one consistent pair:
    // a controller is present only while the pipeline has not reached a terminal
    // phase, so the controller's own view is authoritative whenever it is set.
    let Lifecycle { phase, controller } = state.lifecycle();
    match controller {
        Some(controller) => {
            fn inner_status(
                runtime_desired_status: RuntimeDesiredStatus,
                controller: &Controller,
                default_status: RuntimeStatus,
                storage_status_details: Option<StorageStatusDetails>,
            ) -> ExtendedRuntimeStatus {
                // Error statistics across connectors
                let (inputs, outputs) = retrieve_error_stats(controller.status());
                let mut total_errors = 0u64;
                for endpoint in inputs {
                    total_errors = total_errors
                        .saturating_add(endpoint.metrics.num_transport_errors)
                        .saturating_add(endpoint.metrics.num_parse_errors);
                }
                for endpoint in outputs {
                    total_errors = total_errors
                        .saturating_add(endpoint.metrics.num_encode_errors)
                        .saturating_add(endpoint.metrics.num_transport_errors);
                }
                let connector_stats = ConnectorStats {
                    num_errors: total_errors,
                };

                ExtendedRuntimeStatus {
                    // Order matters: the concurrent-bootstrap phases are more
                    // specific than the (stop-the-world) `bootstrap_in_progress`
                    // flag, which stays false on the concurrent path.
                    runtime_status: match controller.status().concurrent_bootstrap_phase() {
                        ConcurrentBootstrapPhase::Synchronizing => RuntimeStatus::Synchronizing,
                        ConcurrentBootstrapPhase::ConcurrentBootstrapping => {
                            RuntimeStatus::ConcurrentBootstrapping
                        }
                        ConcurrentBootstrapPhase::Inactive => {
                            if controller.status().bootstrap_in_progress() {
                                RuntimeStatus::Bootstrapping
                            } else if controller.is_replaying() {
                                RuntimeStatus::Replaying
                            } else {
                                default_status
                            }
                        }
                    },
                    runtime_status_details: RuntimeStatusDetails {
                        connector_stats: Some(connector_stats),
                        ..Default::default()
                    }
                    .serialize_guaranteed(),
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
                PipelineState::Terminated => terminated_status(
                    controller.status().global_metrics.suspended(),
                    runtime_desired_status,
                    storage_status_details,
                ),
            };
        }
        None => {
            // Controller isn't set.
        }
    };

    // The controller is not set, so the phase says why.
    match phase {
        PipelinePhase::Initializing(inner) => match inner {
            InitializationState::Starting => Ok(ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Initializing,
                runtime_status_details: RuntimeStatusDetails::default().serialize_guaranteed(),
                runtime_desired_status,
                storage_status_details,
            }),
            InitializationState::DownloadingCheckpoint => Ok(ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Initializing,
                runtime_status_details: RuntimeStatusDetails::new_only_reason(
                    "downloading checkpoint from object storage",
                )
                .serialize_guaranteed(),
                runtime_desired_status,
                storage_status_details,
            }),
            InitializationState::Standby => Ok(ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Standby,
                runtime_status_details: RuntimeStatusDetails::default().serialize_guaranteed(),
                runtime_desired_status,
                storage_status_details,
            }),
            InitializationState::AwaitingApproval(diff) => Ok(ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::AwaitingApproval,
                runtime_status_details: RuntimeStatusDetails {
                    approval_diff: Some(serde_json::to_value(&diff).unwrap_or_default()),
                    ..Default::default()
                }
                .serialize_guaranteed(),
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
                    runtime_status_details: RuntimeStatusDetails::default().serialize_guaranteed(),
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
            runtime_status_details: RuntimeStatusDetails::default().serialize_guaranteed(),
            runtime_desired_status,
            storage_status_details,
        }),
    }
}

/// Retrieve whether a pipeline is suspendable or not.
#[get("/suspendable")]
async fn suspendable(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    let reasons = match state.controller()?.can_checkpoint() {
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

#[get("/stats")]
async fn stats(
    state: WebData<ServerState>,
    params: Query<StatsParams>,
) -> Result<HttpResponse, PipelineError> {
    let include_connector_errors = params.into_inner().include_connector_errors;
    Ok(HttpResponse::Ok().json(state.controller()?.api_status(include_connector_errors)))
}

/// Retrieves the error statistics across all endpoints.
fn retrieve_error_stats(
    controller_status: &ControllerStatus,
) -> (
    Vec<EndpointErrorStats<InputEndpointErrorMetrics>>,
    Vec<EndpointErrorStats<OutputEndpointErrorMetrics>>,
) {
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
    (inputs, outputs)
}

/// This endpoint returns a subset of stats that don't need updating and so is more performant than /stats
#[get("/stats/errors")]
async fn error_stats(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    let controller = state.controller()?;
    let controller_status = controller.status();
    let (inputs, outputs) = retrieve_error_stats(controller_status);
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

// Profile downloads.
//
// `/dump_json_profile` delivers a circuit profile.
// The profile size is O(workers x operators).
// The data flow is:
//
//   pipeline process:  [1] workers --> [2] circuit thread --> [3] blocking pool --> [4] actix worker
//   coordinator:       --> [5] (multihost only) combines the hosts' profiles
//   manager service:   --> [6] streaming proxy, or support-data collector
//   client:            --> [7] REST client, or the profiler UI reading a support bundle
//
// [1] Profiler (`dbsp::profile::Profiler`)
//     - runs on: every worker thread of the pipeline process
//     - what it does: walks the circuit and collects each operator's metadata
//     - produces: a `WorkerProfile`, i.e., `Map<operatorId, OperatorMeta>`
//
// [2] Circuit thread (`controller.rs`, `run_commands`)
//     - runs on: the controller's circuit thread, between circuit steps
//     - what it does: `CircuitDriver::json_profile` assembles worker replies
//     - produces: the engine's chunks of the document (for the static engine, a
//       `DbspProfile`, with every worker's `WorkerProfile` plus the circuit
//       `Graph`, which `ProfileJsonWriter` writes one worker per chunk)
//
// [3] Serializer (`json_profile_body`, this file)
//     - runs on: a Tokio blocking-pool thread, one `spawn_blocking` call per chunk
//     - what it does: makes the next chunk
//     - produces: one JSON chunk per worker
//
// [4] HTTP handler (`dump_json_profile`, this file)
//     - runs on: an actix worker thread
//     - what it does: yields each chunk as the client reads; the `Compress`
//       middleware gzips them when the client's `Accept-Encoding` requires it
//     - produces: a chunked HTTP response (never whole in memory), `application/json`,
//       with `Content-Encoding: gzip` when negotiated
//
// [5] Coordinator (`feldera-coordinator`, `crates/coord` in the cloud repository)
//     - runs on: its own process, in multihost pipelines only
//     - what it does: fetches each host's profile and combines them
//     - produces: one JSON response with the hosts' `worker_profiles`
//       concatenated
//
// [6] Pipeline manager, one of two variants
//     - runs on: the manager service
//     - what it does:
//         - the streaming proxy (`runner/interaction.rs`) serves the
//            public `circuit_profile`, `circuit_json_profile` and
//            `heap_profile` endpoints;
//          - the support-data collector (`api/support_data_collector.rs`)
//            fetches the profile on request, or every few minutes for
//            running pipelines
//     - produces:
//          - the proxy: the body unchanged, headers included;
//          - the collector: a database row holding the gzip-compressed JSON,
//            inserted in the support bundle as `circuit_profile.json.gz`
//
// [7] Client, one of two variants
//     - runs on: outside Feldera
//     - what it does:
//          - a REST client (`fda`, `curl`) writes the body to a file;
//          - the profiler UI (`js-packages/profiler-layout`) inflates the
//            `circuit_profile.json.gz` entry of a support bundle and parses the
//            JSON in the browser
//     - produces:
//          - the REST client: a file;
//          - the profiler UI: the profile displayed in the browser
//
// Within the pipeline process:
// - The circuit thread is busy only while the workers collect metadata; the
//   handler awaits the reply and never blocks it.
// - Serialization runs on the blocking pool, one worker at a time; at most one
//   worker's JSON exists in memory, never the whole profile.
// - actix polls the stream only as the client reads, so a slow client stalls
//   the serializer, not the circuit; a disconnected client drops the stream,
//   and no further worker is serialized.

/// The JSON profile as a stream of the engine's chunks.
///
/// Each chunk is made on the blocking pool, in order, and yielded before the
/// next one is started. An error aborts the body, which the client sees as a
/// truncated transfer.
fn json_profile_body(
    mut chunks: JsonProfileChunks,
) -> impl futures::Stream<Item = Result<Bytes, std::io::Error>> {
    async_stream::try_stream! {
        loop {
            let (chunk, rest) = spawn_blocking(move || {
                let chunk = chunks.next();
                (chunk, chunks)
            })
            .await
            .map_err(std::io::Error::other)?;
            chunks = rest;
            match chunk {
                Some(chunk) => yield Bytes::from(chunk?),
                None => break,
            }
        }
    }
}

#[cfg(any(test, feature = "testing"))]
pub mod tests;

#[cfg(test)]
mod json_profile_body_tests {
    use super::*;
    use futures::TryStreamExt;

    /// The body yields the engine's chunks in order, and an error ends it after the chunks
    /// before it.
    #[tokio::test]
    async fn the_body_yields_the_chunks_until_an_error() {
        let chunks: JsonProfileChunks =
            Box::new(vec![Ok(b"{".to_vec()), Ok(b"}".to_vec())].into_iter());
        let body: Vec<Bytes> = json_profile_body(chunks).try_collect().await.unwrap();
        assert_eq!(body, vec![Bytes::from("{"), Bytes::from("}")]);

        let chunks: JsonProfileChunks = Box::new(
            vec![
                Ok(b"{".to_vec()),
                Err(std::io::Error::other("no profile")),
                Ok(b"}".to_vec()),
            ]
            .into_iter(),
        );
        let mut body = std::pin::pin!(json_profile_body(chunks));
        assert_eq!(body.try_next().await.unwrap(), Some(Bytes::from("{")));
        assert_eq!(body.try_next().await.unwrap_err().to_string(), "no profile");
    }
}

#[get("/dump_profile")]
async fn dump_profile(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    let profile = state.controller()?.async_graph_profile().await?;
    // `ZipWriter` needs a synchronous `Write` sink, so the archive goes to an
    // anonymous temporary file (removed when closed) and is streamed from there;
    // memory consumption stays bounded.
    let body = async_stream::try_stream! {
        let file = spawn_blocking(move || -> std::io::Result<std::fs::File> {
            let mut file = tempfile::tempfile()?;
            profile.write_zip(&mut file)?;
            std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(0))?;
            Ok(file)
        })
        .await
        .map_err(std::io::Error::other)??;
        let mut chunks = tokio_util::io::ReaderStream::new(tokio::fs::File::from_std(file));
        while let Some(chunk) = futures::StreamExt::next(&mut chunks).await {
            yield chunk?;
        }
    };
    Ok(HttpResponse::Ok()
        .insert_header(header::ContentType("application/zip".parse().unwrap()))
        .insert_header(header::ContentDisposition::attachment("profile.zip"))
        .insert_header(header::ContentEncoding::Identity)
        .streaming::<_, std::io::Error>(body))
}

/// Registered in [`build_app`] behind a `Compress` middleware, which negotiates
/// the response encoding with the client.
async fn dump_json_profile(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    let profile = state.controller()?.async_json_profile().await?;
    Ok(HttpResponse::Ok()
        .insert_header(header::ContentType("application/json".parse().unwrap()))
        .insert_header(header::ContentDisposition::attachment("profile.json"))
        .streaming::<_, std::io::Error>(json_profile_body(profile)))
}

/// Dump the low-level IR of the circuit, or answer 501 Not Implemented if the engine keeps
/// none.
#[get("/lir")]
async fn lir(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    Ok(HttpResponse::Ok()
        .insert_header(header::ContentType("application/zip".parse().unwrap()))
        .insert_header(header::ContentDisposition::attachment("lir.zip"))
        .body(state.controller()?.lir()?.as_zip()))
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
        Some(storage) => storage.checkpoints()?,
        None => Default::default(),
    })
}

/// Starts syncing the checkpoint named by `uuid` to object storage, in the
/// background.
///
/// A sync already running for `uuid` coalesces with it: this starts no second
/// sync and reports no error, because the sync the caller asked for is under
/// way.  Both callers answer `202 Accepted` either way, so a retried request
/// means "the sync is in progress", not "a fresh sync started".
fn sync_checkpoint(state: WebData<ServerState>, controller: Controller, uuid: Uuid) {
    /// Tracks state for an ongoing checkpoint sync.
    struct CheckpointSyncGuard {
        state: WebData<ServerState>,
        uuid: Option<Uuid>,
    }

    impl CheckpointSyncGuard {
        /// Tries to create a new `CheckpointSyncGuard` for `uuid`, and succeeds if
        /// there is not already one for that UUID.
        fn try_new(state: &WebData<ServerState>, uuid: Uuid) -> Option<Self> {
            state
                .sync_checkpoint_status()
                .running
                .insert(uuid)
                .then(|| Self {
                    state: state.clone(),
                    uuid: Some(uuid),
                })
        }

        /// Records the checkpoint sync of `uuid` as complete.
        fn completed(mut self, result: Result<(), Arc<ControllerError>>) {
            // Prevent the `Drop` implementation from trying to delete the UUID
            // again.
            let uuid = self
                .uuid
                .take()
                .expect("CheckpointSyncGuard has not yet been dropped");

            let mut status = self.state.sync_checkpoint_status();

            // Remove `uuid` from the running sync.
            if !status.running.remove(&uuid) {
                error!("checkpoint sync for {uuid} unexpectedly already completed");
            }

            // Record the outcome.
            //
            // `success` and `failure` each name the last sync to reach that
            // outcome, so re-syncing a checkpoint would otherwise leave the same
            // UUID in both with nothing to say which came last.  Recording an
            // outcome therefore clears the opposite one when it names `uuid`: the
            // newer result replaces the stale one.
            match result {
                Ok(()) => {
                    status.success = Some(uuid);
                    status.failure.take_if(|failure| failure.uuid == uuid);
                }
                Err(e) => {
                    status.failure = Some(CheckpointSyncFailure {
                        uuid,
                        error: e.to_string(),
                    });
                    if status.success == Some(uuid) {
                        status.success = None;
                    }
                }
            }
        }
    }

    impl Drop for CheckpointSyncGuard {
        fn drop(&mut self) {
            if let Some(uuid) = self.uuid.take() {
                self.state.sync_checkpoint_status().running.remove(&uuid);
            }
        }
    }

    if let Some(guard) = CheckpointSyncGuard::try_new(&state, uuid) {
        spawn(async move {
            let result = controller.async_sync_checkpoint(uuid).await;
            guard.completed(result);
        });
    }
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

    let incarnation_uuid = state.incarnation_uuid;
    sync_checkpoint(state, controller, last_checkpoint);

    Ok(HttpResponse::Accepted().json(CheckpointSyncResponse::new(
        last_checkpoint,
        incarnation_uuid,
    )))
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

    let incarnation_uuid = state.incarnation_uuid;
    sync_checkpoint(state, controller, uuid);

    Ok(HttpResponse::Accepted().json(CheckpointSyncResponse::new(uuid, incarnation_uuid)))
}

/// Checks a client-supplied incarnation UUID (from `CheckpointStatusQuery`).
///
/// `requested` is `None` for old clients that don't supply an UUID.
fn check_incarnation_uuid(
    state: &ServerState,
    requested: Option<Uuid>,
) -> Result<(), PipelineError> {
    if let Some(requested) = requested
        && requested != state.incarnation_uuid
    {
        return Err(PipelineError::IncarnationUuidMismatch {
            requested,
            expected: state.incarnation_uuid,
        });
    }
    Ok(())
}

/// Initiates a checkpoint and returns its sequence number.  The caller may poll
/// `/checkpoint_status` to determine when the checkpoint completes.
#[post("/checkpoint")]
async fn checkpoint(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    let controller = state.controller()?;
    let seq = state.checkpoint_state.lock().unwrap().next_seq();
    let incarnation_uuid = state.incarnation_uuid;
    spawn(async move {
        let result = controller.async_checkpoint().await;
        state
            .checkpoint_state
            .lock()
            .unwrap()
            .completed(seq, result.map(|c| c.circuit));
    });
    Ok(HttpResponse::Ok().json(CheckpointResponse::new(seq, incarnation_uuid)))
}

#[get("/checkpoint_status")]
async fn checkpoint_status(
    state: WebData<ServerState>,
    params: web::Query<CheckpointStatusQuery>,
) -> Result<HttpResponse, PipelineError> {
    check_incarnation_uuid(&state, params.incarnation_uuid)?;
    let status = state.checkpoint_state.lock().unwrap().status.clone();
    Ok(HttpResponse::Ok().json(status))
}

#[get("/checkpoints")]
async fn checkpoints(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    Ok(HttpResponse::Ok().json(get_checkpoints(&state)?))
}

/// List checkpoints available in the configured remote object storage.
#[get("/checkpoints/remote")]
async fn remote_checkpoints(state: WebData<ServerState>) -> Result<HttpResponse, PipelineError> {
    // Sync is configured only with storage.
    let (Some(storage), Some(sync)) = (state.storage.clone(), state.sync_config.clone()) else {
        return Err(PipelineError::ControllerError {
            error: Arc::new(ControllerError::checkpoint_fetch_error(
                "listing remote checkpoints requires sync to be configured".to_string(),
            )),
        });
    };

    let result =
        spawn_blocking(move || crate::controller::sync::list_remote_checkpoints(&*storage, &sync))
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
    params: web::Query<CheckpointStatusQuery>,
) -> Result<HttpResponse, PipelineError> {
    check_incarnation_uuid(&state, params.incarnation_uuid)?;
    let mut sync_status = state.sync_checkpoint_status();

    let controller = state.controller()?;
    if let Some(chk) = controller.last_checkpoint_sync().id {
        sync_status.periodic = Some(chk);
    }

    Ok(HttpResponse::Ok().json(sync_status.clone()))
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
                let mut suspend_error = None;
                loop {
                    if let Ok(controller) = state.controller() {
                        if let Err(error) = controller.async_suspend().await {
                            error!("controller suspend failed ({error})");
                            suspend_error = Some(error);
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
                // A failed suspend leaves the pipeline unsuspended: `suspended`
                // reports it as a fatal error so the pipeline manager records the
                // failure rather than a clean suspend. The circuit is deliberately
                // left running on failure (see the `SuspendCommand` handler in
                // `controller.rs`), so `/status` never observed a `Terminated`
                // circuit to mask as `Suspended`; this phase is what the poll sees
                // once the controller is deallocated.
                if let Some(controller) = state.suspended(suspend_error)
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

/// Rejects a configuration that `/testing/load` cannot swap in: anything that
/// keeps state outside the process (storage, fault tolerance) or spans hosts.
fn check_program_loadable(
    state: &ServerState,
    config: &PipelineConfig,
) -> Result<(), ControllerError> {
    let reason = if state.storage.is_some() || config.storage().is_some() {
        Some("pipelines with storage")
    } else if config.global.fault_tolerance.is_enabled() {
        Some("fault-tolerant pipelines")
    } else if config.multihost.is_some() || config.global.hosts > 1 {
        Some("multihost pipelines")
    } else {
        None
    };
    match reason {
        Some(reason) => Err(ControllerError::pipeline_config_parse_error(&format!(
            "'/testing/load' does not support {reason}"
        ))),
        None => Ok(()),
    }
}

/// The query parameters of `POST /testing/load`.
#[derive(Debug, Deserialize)]
struct LoadProgramArgs {
    /// The shared library with the program's Rust UDFs, a path on the pipeline's
    /// host, as `--udf-library` names it at startup.
    #[serde(default)]
    udf_library: Option<PathBuf>,
}

/// Replaces the running program with the one in the request's pipeline
/// configuration, without restarting the process.  Served only with
/// `--enable-program-load`.
///
/// The body is the pipeline configuration, the same document as
/// `--config-file`; the query parameter `udf_library` names the program's UDF
/// library.  A configuration that does not load leaves the current program
/// running.  Otherwise the current controller stops and the new program
/// bootstraps in the background; poll `/status` for `Running` as after a
/// process start.
#[post("/testing/load")]
async fn load_program(
    state: WebData<ServerState>,
    args: Query<LoadProgramArgs>,
    body: web::Bytes,
) -> Result<HttpResponse, PipelineError> {
    let program_load = state.program_load.get().ok_or_else(|| {
        ControllerError::not_supported("this pipeline does not load programs at run time")
    })?;
    let config: PipelineConfig = serde_json::from_slice(&body).map_err(|e| {
        ControllerError::pipeline_config_parse_error(&format!(
            "invalid pipeline configuration: {e}"
        ))
    })?;
    check_program_loadable(&state, &config)?;
    let circuit_factory = program_load
        .loader
        .load(&config, args.udf_library.as_deref())?;
    let builder = ControllerBuilder::new(&config, &program_load.open_storage)?;

    if let Some(controller) = state.reset_for_program_load()?
        && let Err(error) = controller.async_stop().await
    {
        warn!("Stopping the replaced program failed ({error})");
    }
    let generation = state.program_generation();
    info!(
        "Loading program '{}' (generation {generation})",
        config.name.as_deref().unwrap_or_default()
    );
    thread::Builder::new()
        .name("pipeline-init".to_string())
        .spawn({
            let state = state.clone();
            move || bootstrap(builder, circuit_factory, state)
        })
        .map_err(|e| ControllerError::io_error("spawning the pipeline initialization thread", e))?;
    Ok(HttpResponse::Accepted().json(json!({ "generation": generation })))
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
    /// Connector metadata for every record of this request: a JSON object
    /// that `CONNECTOR_METADATA()` returns for the records.
    connector_metadata: Option<String>,
}

impl IngressArgs {
    /// Names of the query parameters that the `/ingress` handler consumes.
    fn names() -> &'static [&'static str] {
        field_names::<Self>()
    }
}

/// The query string of `request` without the parameters named in
/// `handler_args`.  The remaining parameters configure the data format.
fn format_query(request: &HttpRequest, handler_args: &[&str]) -> String {
    let mut format_args = form_urlencoded::Serializer::new(String::new());
    for (name, value) in form_urlencoded::parse(request.query_string().as_bytes()) {
        if !handler_args.contains(&name.as_ref()) {
            format_args.append_pair(&name, &value);
        }
    }
    format_args.finish()
}

/// The field names that the `Deserialize` implementation of the struct `T`
/// accepts, which are the query parameters that a handler taking
/// `web::Query<T>` consumes.
///
/// `#[derive(Deserialize)]` writes the field names into the generated
/// implementation as the `fields` argument of `Deserializer::deserialize_struct`.
/// For example, for `IngressArgs` the derive generates
///
/// ```ignore
/// const FIELDS: &'static [&'static str] = &["format", "force"];
/// _serde::Deserializer::deserialize_struct(deserializer, "IngressArgs", FIELDS, visitor)
/// ```
fn field_names<T: for<'de> Deserialize<'de>>() -> &'static [&'static str] {
    use serde::de::{Error, Visitor};
    use std::fmt::{Display, Formatter};

    /// A deserializer that fails as soon as it learns the field names of the
    /// struct it is asked for, and reports them as its error.
    struct FieldNamesDeserializer;

    #[derive(Debug)]
    struct Fields(&'static [&'static str]);

    impl Display for Fields {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }

    impl std::error::Error for Fields {}

    impl Error for Fields {
        fn custom<M: Display>(_: M) -> Self {
            Fields(&[])
        }
    }

    impl<'de> serde::Deserializer<'de> for FieldNamesDeserializer {
        type Error = Fields;

        fn deserialize_struct<V: Visitor<'de>>(
            self,
            _name: &'static str,
            fields: &'static [&'static str],
            _visitor: V,
        ) -> Result<V::Value, Fields> {
            Err(Fields(fields))
        }

        fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Fields> {
            Err(Fields(&[]))
        }

        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
            bytes byte_buf option unit unit_struct newtype_struct seq tuple
            tuple_struct map enum identifier ignored_any
        }
    }

    match T::deserialize(FieldNamesDeserializer) {
        Err(Fields(fields)) => fields,
        Ok(_) => unreachable!("a struct does not deserialize from the field-name deserializer"),
    }
}

/// Lookup or create an HTTP input endpoint.
fn get_or_create_http_input_endpoint(
    state: &WebData<ServerState>,
    format: FormatConfig,
    table_name: String,
    endpoint_name: String,
) -> Result<HttpInputEndpoint, PipelineError> {
    // Serialize lookup-or-create.
    //
    // Adding an input endpoint is almost atomic.  However, there's a gap in
    // [ControllerStatus::insert_input_endpoint] when the new endpoint is
    // visible but its reader is still `None`.  Until we plug that gap, it's
    // easiest to just serialize creating HTTP input endpoints.
    static SERIALIZE: Mutex<()> = Mutex::new(());
    let _guard = SERIALIZE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

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

/// The HTTP input endpoints one actix worker has created, keyed by endpoint name.
/// They feed the controller of one program generation, which `/testing/load`
/// replaces, so a newer generation empties the cache.
struct IngressEndpointCache {
    generation: u64,
    endpoints: HashMap<String, HttpInputEndpoint, BuildHasherDefault<DefaultHasher>>,
}

impl IngressEndpointCache {
    const fn new() -> Self {
        Self {
            generation: 0,
            endpoints: HashMap::with_hasher(BuildHasherDefault::new()),
        }
    }

    fn get(&mut self, generation: u64, name: &str) -> Option<HttpInputEndpoint> {
        if self.generation != generation {
            self.endpoints.clear();
            self.generation = generation;
        }
        self.endpoints.get(name).cloned()
    }

    /// Keeps `endpoint` unless another program was loaded while it was created.
    fn insert(&mut self, generation: u64, name: String, endpoint: HttpInputEndpoint) {
        if self.generation == generation {
            self.endpoints.insert(name, endpoint);
        }
    }
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
        static TABLE_ENDPOINTS: RefCell<IngressEndpointCache> = const {
            RefCell::new(IngressEndpointCache::new())
        };
    }
    debug!("{req:?}");

    let table_name = path.into_inner();

    let connector_metadata = args
        .connector_metadata
        .as_deref()
        .map(RequestMetadata::parse)
        .transpose()
        .map_err(|error| PipelineError::InvalidParam { error })?;

    // Generate deterministic endpoint name per (table_name, FormatConfig).
    let parser_endpoint_name = format!("{table_name}.api-ingress-{}", args.format);
    let format = parser_config_from_http_request(&parser_endpoint_name, &args.format, &req)?;

    let mut endpoint_hasher = DefaultHasher::new();
    table_name.hash(&mut endpoint_hasher);
    format.hash(&mut endpoint_hasher);
    let endpoint_hash = endpoint_hasher.finish();

    let endpoint_name = format!("{table_name}.api-ingress-{endpoint_hash:016x}");

    let generation = state.program_generation();
    let cached_endpoint =
        TABLE_ENDPOINTS.with_borrow_mut(|cache| cache.get(generation, &endpoint_name));
    let endpoint = match cached_endpoint {
        Some(endpoint) => endpoint,
        None => {
            let endpoint = get_or_create_http_input_endpoint(
                &state,
                format.clone(),
                table_name.clone(),
                endpoint_name.clone(),
            )?;
            TABLE_ENDPOINTS
                .with_borrow_mut(|cache| cache.insert(generation, endpoint_name, endpoint.clone()));
            endpoint
        }
    };

    // Call endpoint to complete request.
    endpoint
        .complete_request(payload, args.force, connector_metadata)
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

    let config = format
        .config_from_http_query(endpoint_name, &format_query(request, IngressArgs::names()))?;

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
/// HTTP request using the `OutputFormat::config_from_http_query` method.
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

    let config = format
        .config_from_http_query(endpoint_name, &format_query(request, EgressArgs::names()))?;

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

impl EgressArgs {
    /// Names of the query parameters that the `/egress` handler consumes.
    fn names() -> &'static [&'static str] {
        field_names::<Self>()
    }
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

    let controller = state.controller()?;

    // Create HTTP endpoint.  In `backpressure` mode it waits for the client to
    // receive each chunk, so it needs the controller's shutdown token to stop
    // waiting when the pipeline goes down.
    let endpoint_name = format!("{}.api-{}", config.stream, Uuid::new_v4());
    let endpoint = HttpOutputEndpoint::new(
        &endpoint_name,
        format,
        http_output_config.backpressure,
        controller.shutdown_token().clone(),
    );
    // Pre-connect the streaming receiver before `add_output_endpoint` so the
    // initial snapshot (when `send_snapshot` is enabled) is captured rather
    // than racing the streaming body that drains it.
    let response_receiver = endpoint.connect_stream();

    // Connect endpoint.
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

/// Pause the output endpoint: it discards the output it receives from now on.
/// Output it has already received is still transmitted.
#[get("/output_endpoints/{endpoint_name}/pause")]
async fn pause_output_endpoint(
    state: WebData<ServerState>,
    path: web::Path<String>,
) -> Result<HttpResponse, PipelineError> {
    state.controller()?.pause_output_endpoint(&path)?;
    Ok(HttpResponse::Ok().into())
}

/// Resume the output endpoint.  Output produced while it was paused is gone;
/// the endpoint resumes with the next output the pipeline produces.
#[get("/output_endpoints/{endpoint_name}/start")]
async fn start_output_endpoint(
    state: WebData<ServerState>,
    path: web::Path<String>,
) -> Result<HttpResponse, PipelineError> {
    state.controller()?.start_output_endpoint(&path)?;
    Ok(HttpResponse::Ok().into())
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
            None => get_status(&state, false),
            Some(prev) => loop {
                let notify = state.desired_status_change.notified();
                let status = get_status(&state, false);
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
                capabilities: CoordinationCapabilities::current(),
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

    let Some(storage) = state.storage.clone() else {
        return Err(PipelineError::ControllerError {
            error: Arc::new(ControllerError::checkpoint_fetch_error(
                "checkpoint pull requires storage to be configured".to_string(),
            )),
        });
    };
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

    let pipeline =
        state
            .pipeline_identity
            .clone()
            .ok_or_else(|| PipelineError::ControllerError {
                error: Arc::new(ControllerError::checkpoint_fetch_error(
                    missing_pipeline_identity_message("checkpoint pull requires pipeline identity"),
                )),
            })?;

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
            crate::controller::sync::pull_once(&*storage, &sync, host_info, standby, &pipeline)
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

/// Reports the values that this host's distributed input connectors chose for
/// all the hosts to use.  The coordinator reads this from host 0 before it
/// activates the other hosts (see `CoordinationActivate::input_choices`).
///
/// This is available while the pipeline is still initializing, as soon as the
/// connectors have initialized.  Before that, it fails like other endpoints
/// that need a controller.
#[get("/coordination/input/choices")]
async fn coordination_input_choices(
    state: WebData<ServerState>,
) -> Result<HttpResponse, PipelineError> {
    let choices = state.input_choices.lock().unwrap().clone();
    let choices = match choices {
        Some(choices) => choices,
        None => state.controller()?.input_choices(),
    };
    Ok(HttpResponse::Ok().json(InputChoices { choices }))
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

/// The status that the server stores, so that a restarted server resumes it.
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredStatus {
    /// Desired status.
    desired_status: RuntimeDesiredStatus,

    /// Bootstrap policy.
    bootstrap_policy: BootstrapPolicy,

    /// Bootstrap the pipeline with output connectors disabled.
    #[serde(default, skip_serializing_if = "is_false")]
    silent_bootstrap: bool,

    /// Bootstrap new and modified views concurrently.
    #[serde(default, skip_serializing_if = "is_false")]
    concurrent_bootstrap: bool,

    // Deployment ID.
    deployment_id: Uuid,
}

fn is_false(value: &bool) -> bool {
    !value
}

/// Helpers shared between the HTTP-only and Kafka-using server tests. Lives
/// outside `mod test_with_kafka` so it stays available when `with-kafka` is
/// off.
#[cfg(test)]
mod test_status {
    use super::tests::json_config_file;
    use super::{ServerState, error_handler, get_status, parse_config, terminated_status};
    use crate::{
        ControllerError,
        server::{InitializationState, PipelinePhase},
    };
    use actix_web::web::Data as WebData;
    use feldera_types::runtime_status::{
        BootstrapConfig, BootstrapPolicy, RuntimeDesiredStatus, RuntimeStatus,
    };
    use serde_json::json;
    use std::{io::Write, sync::Arc};
    use uuid::Uuid;

    /// The configuration file is read as JSON whatever its name: the YAML
    /// fallback for older launchers is gone, and the error says so and
    /// names the file.
    #[test]
    fn parse_config_reads_json_only() {
        let mut yaml_file = tempfile::Builder::new().suffix(".yaml").tempfile().unwrap();
        yaml_file
            .write_all(b"name: test\ninputs:\noutputs:\n")
            .unwrap();
        let error = parse_config(yaml_file.path()).unwrap_err().to_string();
        assert!(
            error.contains(&yaml_file.path().display().to_string()),
            "{error}"
        );
        assert!(error.contains("must be JSON"), "{error}");
        let json_file = json_config_file(&json!({"name": "test"}));
        let config = parse_config(json_file.path()).unwrap();
        assert_eq!(config.name.as_deref(), Some("test"));
    }

    /// A successful suspend terminates the circuit before the server reports
    /// `PipelinePhase::Suspended`, so `terminated_status` must report a clean
    /// `Suspended` while a suspend is in progress instead of the spurious
    /// `PipelineTerminated` error that flaked `suspend_and_resume_demos`.
    #[test]
    fn terminated_status_reports_suspended_while_suspending() {
        let status = terminated_status(true, RuntimeDesiredStatus::Suspended, None)
            .expect("a suspending pipeline must not surface PipelineTerminated");
        assert!(matches!(status.runtime_status, RuntimeStatus::Suspended));

        // An unexpected termination stays a fatal error.
        let error = terminated_status(false, RuntimeDesiredStatus::Running, None)
            .expect_err("an unexpected termination must remain PipelineTerminated");
        assert_eq!(error.error.error_code.as_ref(), "PipelineTerminated");
    }

    /// A circuit that dies while a suspend is pending must not be reported as a
    /// clean suspend.
    ///
    /// Requesting a suspend does not make the termination a suspend: the circuit
    /// thread also terminates the pipeline when it exits with an error that
    /// `is_fatal_controller_error` does not classify as fatal, and then nothing
    /// has written a checkpoint. Reporting `Suspended` would have the pipeline
    /// manager record a successful suspend and later resume from a checkpoint
    /// that does not exist, so only the reason recorded by the `SuspendCommand`
    /// handler may produce it.
    #[test]
    fn terminated_status_reports_a_death_during_suspend_as_an_error() {
        let error = terminated_status(false, RuntimeDesiredStatus::Suspended, None)
            .expect_err("a pipeline that died while suspending must not report a clean suspend");
        assert_eq!(error.error.error_code.as_ref(), "PipelineTerminated");
    }

    /// `/status` must report a fatal controller error that arrives before the
    /// controller is published.
    ///
    /// The circuit thread can fail before `do_bootstrap` reaches
    /// `set_controller`, so recording the failure cannot depend on there being a
    /// controller to deallocate; otherwise the error is dropped and the pipeline
    /// goes on to report itself as initialized.
    #[test]
    fn fatal_error_before_controller_is_published_is_reported() {
        let state = WebData::new(ServerState::new(
            PipelinePhase::Initializing(InitializationState::Starting),
            String::default(),
            RuntimeDesiredStatus::Paused,
            BootstrapConfig::from(BootstrapPolicy::Allow),
            Uuid::now_v7(),
            None,
            None,
            None,
            None,
        ));

        let weak = Arc::downgrade(&state.clone().into_inner());
        error_handler(
            &weak,
            state.program_generation(),
            Arc::new(ControllerError::DbspPanic),
            None,
        );

        let error = get_status(&state, false)
            .expect_err("a pipeline that hit a fatal error must report an error");
        assert_eq!(error.error.error_code.as_ref(), "DbspPanic");
    }

    /// A panic on the controller's circuit thread must fail the pipeline.
    ///
    /// The circuit thread cannot recover from a panic, so a pipeline that kept
    /// reporting `Running` afterward would hide a dead pipeline from
    /// orchestration and monitoring.
    #[test]
    fn controller_panic_fails_the_pipeline() {
        let state = WebData::new(ServerState::new(
            PipelinePhase::Initializing(InitializationState::Starting),
            String::default(),
            RuntimeDesiredStatus::Running,
            BootstrapConfig::from(BootstrapPolicy::Allow),
            Uuid::now_v7(),
            None,
            None,
            None,
            None,
        ));

        let weak = Arc::downgrade(&state.clone().into_inner());
        error_handler(
            &weak,
            state.program_generation(),
            Arc::new(ControllerError::ControllerPanic),
            None,
        );

        assert!(matches!(state.phase(), PipelinePhase::Failed(_)));
        let error = get_status(&state, false)
            .expect_err("a pipeline whose circuit thread panicked must report an error");
        assert_eq!(error.error.error_code.as_ref(), "ControllerPanic");
    }

    /// A suspend that overlaps a fatal controller error must not report a clean
    /// suspend.
    ///
    /// The `/suspend` task waits for a controller to suspend and treats a
    /// terminal phase as its cue to stop waiting.  It then reaches `suspended`
    /// with no suspend error of its own to report, so
    /// `PipelinePhase::Suspended` must not replace the recorded failure: the
    /// pipeline failed and wrote no checkpoint, and the pipeline manager would
    /// otherwise record a successful suspend and later try to resume from it.
    #[test]
    fn suspend_does_not_overwrite_a_fatal_error() {
        let state = WebData::new(ServerState::new(
            PipelinePhase::Initializing(InitializationState::Starting),
            String::default(),
            RuntimeDesiredStatus::Suspended,
            BootstrapConfig::from(BootstrapPolicy::Allow),
            Uuid::now_v7(),
            None,
            None,
            None,
            None,
        ));

        let weak = Arc::downgrade(&state.clone().into_inner());
        error_handler(
            &weak,
            state.program_generation(),
            Arc::new(ControllerError::DbspPanic),
            None,
        );
        // What the `/suspend` task records once it stops waiting for a controller,
        // having no suspend error of its own to report.
        assert!(state.suspended(None).is_none());

        let error = get_status(&state, false)
            .expect_err("a pipeline that hit a fatal error must report an error");
        assert_eq!(error.error.error_code.as_ref(), "DbspPanic");
    }
}

/// The server's side of `POST /testing/load`: program generations, the flag, and the
/// ingress cache.
#[cfg(test)]
mod test_program_load {
    use super::{
        IngressEndpointCache, InitializationState, PipelineError, PipelinePhase, ServerArgs,
        ServerState, run_server,
    };
    use crate::ControllerError;
    use crate::engine::{CircuitFactory, OpenStorageFn};
    use crate::transport::http::HttpInputEndpoint;
    use feldera_types::config::PipelineConfig;
    use feldera_types::runtime_status::{BootstrapConfig, BootstrapPolicy, RuntimeDesiredStatus};
    use feldera_types::transport::http::HttpInputConfig;
    use std::sync::Arc;
    use uuid::Uuid;

    fn state(phase: PipelinePhase, desired_status: RuntimeDesiredStatus) -> ServerState {
        ServerState::new(
            phase,
            String::default(),
            desired_status,
            BootstrapConfig::from(BootstrapPolicy::Allow),
            Uuid::new_v4(),
            None,
            None,
            None,
            None,
        )
    }

    /// A fatal error from a program that `/testing/load` replaced leaves the new
    /// program alone, while an error from the current program still fails it.
    #[test]
    fn late_error_from_a_replaced_program_does_not_fail_its_replacement() {
        let state = state(
            PipelinePhase::InitializationComplete,
            RuntimeDesiredStatus::Running,
        );
        let replaced_generation = state.program_generation();
        assert!(state.reset_for_program_load().unwrap().is_none());

        assert!(
            state
                .fail(replaced_generation, Arc::new(ControllerError::DbspPanic))
                .is_none()
        );
        assert!(matches!(state.phase(), PipelinePhase::Initializing(_)));

        state.fail(
            state.program_generation(),
            Arc::new(ControllerError::DbspPanic),
        );
        assert!(matches!(state.phase(), PipelinePhase::Failed(_)));
    }

    /// A load waits for an initialization in flight, and needs a running or paused
    /// pipeline.
    #[test]
    fn program_load_needs_an_initialized_running_or_paused_pipeline() {
        let initializing = state(
            PipelinePhase::Initializing(InitializationState::Starting),
            RuntimeDesiredStatus::Running,
        );
        assert!(matches!(
            initializing.reset_for_program_load(),
            Err(PipelineError::Initializing)
        ));
        assert_eq!(initializing.program_generation(), 0);

        let suspended = state(
            PipelinePhase::InitializationComplete,
            RuntimeDesiredStatus::Suspended,
        );
        assert!(matches!(
            suspended.reset_for_program_load(),
            Err(PipelineError::InvalidTransition(
                "load a program",
                RuntimeDesiredStatus::Suspended
            ))
        ));
        assert_eq!(suspended.program_generation(), 0);
    }

    /// An engine without a program loader refuses `--enable-program-load` before the
    /// server reads its configuration.
    #[test]
    fn an_engine_without_a_loader_refuses_enable_program_load() {
        let args = ServerArgs {
            config_file: "/no/such/config.json".to_string(),
            metadata_file: None,
            bind_address: "127.0.0.1".to_string(),
            default_port: None,
            storage_location: None,
            enable_https: false,
            https_tls_cert_path: None,
            https_tls_key_path: None,
            initial: RuntimeDesiredStatus::Running,
            bootstrap_policy: BootstrapPolicy::Allow,
            silent_bootstrap: false,
            concurrent_bootstrap: false,
            deployment_id: Uuid::new_v4(),
            host_id: None,
            enable_program_load: true,
        };
        let open_storage: OpenStorageFn =
            Arc::new(|_| Err(ControllerError::not_supported("no storage")));
        let error = run_server(
            args,
            Box::new(
                |_: &PipelineConfig| -> Result<CircuitFactory, ControllerError> {
                    unreachable!("the server refuses the flag first")
                },
            ),
            open_storage,
            None,
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Error parsing command line arguments: This pipeline's engine cannot load a \
             program at run time, so it does not accept '--enable-program-load'."
        );
    }

    /// A newer program generation empties the ingress endpoint cache, and an endpoint
    /// created while another program loaded is not kept.
    #[test]
    fn ingress_endpoint_cache_follows_the_program_generation() {
        let endpoint = || {
            HttpInputEndpoint::new(HttpInputConfig {
                name: "t".to_string(),
            })
        };
        let mut cache = IngressEndpointCache::new();
        assert!(cache.get(0, "t").is_none());
        cache.insert(0, "t".to_string(), endpoint());
        assert!(cache.get(0, "t").is_some());

        assert!(cache.get(1, "t").is_none());
        cache.insert(0, "t".to_string(), endpoint());
        assert!(cache.get(1, "t").is_none());
        cache.insert(1, "t".to_string(), endpoint());
        assert!(cache.get(1, "t").is_some());
    }
}

/// Server tests that exercise HTTP egress/ingress only — no Kafka required.
#[cfg(test)]
mod test_http {

    /// The `/ingress` handler must not pass its own query parameters to the
    /// format.
    #[cfg(feature = "with-avro")]
    #[test]
    fn test_ingress_query_excludes_handler_args() {
        use super::{IngressArgs, format_query, parser_config_from_http_request};
        use actix_web::test::TestRequest;

        let request = TestRequest::with_uri(
            "/ingress/t?format=avro&force=true&update_format=raw&skip_schema_id=true",
        )
        .to_http_request();
        assert_eq!(
            format_query(&request, IngressArgs::names()),
            "update_format=raw&skip_schema_id=true"
        );
        parser_config_from_http_request("t", "avro", &request).unwrap();

        let request =
            TestRequest::with_uri("/ingress/t?format=avro&update_formt=raw").to_http_request();
        let error = parser_config_from_http_request("t", "avro", &request).unwrap_err();
        assert!(error.to_string().contains("update_formt"), "{error}");

        // Percent-encoded values and repeated parameters reach the format
        // unchanged.
        let request =
            TestRequest::with_uri("/ingress/t?schema=%7B%22a%22%3A1%7D&force=true&x=1&x=2")
                .to_http_request();
        let pairs: Vec<(String, String)> =
            form_urlencoded::parse(format_query(&request, IngressArgs::names()).as_bytes())
                .into_owned()
                .collect();
        assert_eq!(
            pairs,
            [("schema", "{\"a\":1}"), ("x", "1"), ("x", "2")]
                .map(|(name, value)| (name.to_string(), value.to_string()))
        );
    }

    /// The handler parameter names come from the `IngressArgs` and
    /// `EgressArgs` structs themselves.
    #[test]
    fn test_handler_arg_names() {
        use super::{EgressArgs, IngressArgs};

        assert_eq!(
            IngressArgs::names(),
            ["format", "force", "connector_metadata"]
        );
        assert_eq!(
            EgressArgs::names(),
            ["backpressure", "format", "send_snapshot"]
        );
    }
}
