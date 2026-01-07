//! Interface between the coordinator and the pipeline.
//!
//! To support multihost pipelines, a coordinator process interposes between the
//! pipeline manager and each of the pipeline processes.  To the pipeline
//! manager, the coordinator presents the same interface as a single-host
//! pipeline.  To the pipelines below it, the coordinator needs some additional
//! interface endpoints, defined in this module.  These endpoints use paths that
//! begin with `/coordination`.
//!
//! # Startup
//!
//! At startup, a pipeline within a multihost pipeline enters a special
//! [RuntimeStatus::Coordination] state, in which it waits for instructions from
//! the coordinator.  When it is ready, the coordinator sends a
//! [CoordinationActivate] request to properly start the pipeline.
//!
//! [RuntimeStatus::Coordination]: crate::runtime_status::RuntimeStatus::Coordination
//!
//! # Steps
//!
//! Once it is activated, a multihost pipeline behaves differently from
//! single-host regarding running circuit steps.  The pipelines do not take any
//! steps on their own.  Rather, the coordinator is responsible for coordinating
//! individual steps.  The coordinator sends [StepRequest] to trigger steps,
//! while reading a stream of [StepStatus] updates to find out the effects.
//!
//! The coordinator is responsible for enabling and disabling input connector
//! buffering using `/start` and `/pause`.  In multihost mode, these control
//! buffering but not running steps.
//!
//! # Checkpointing
//!
//! The coordinator is responsible for coordinating checkpoints as well.  It
//! reads a stream of [CheckpointCoordination] updates from each pipeline to
//! track the status.  To execute a checkpoint, it calls
//! `/coordination/checkpoint/prepare` on each pipeline.  The pipelines may
//! update their status to indicate that some of their input connectors have
//! barriers; if so, then the coordinator should force steps until the barriers
//! are cleared.  When all of the pipelines are ready, the coordinator uses
//! `/coordination/checkpoint/release` to trigger it.  Then the coordinator
//! waits for all of the checkpoints to complete, or for at least one to fail.
//!
//! # Transactions
//!
//! The coordinator is responsible for coordinating transactions.  It reads a
//! stream of [TransactionCoordination] updates from each pipeline.  It merges
//! the set of transactions requested by input connectors from these updates
//! with those requested through its own API from the pipeline manager, and in
//! turn uses the same API to start and commit transactions in the pipelines.
//! The pipelines only use transactions started through the API, as instructed
//! by the coordinator; they report input connector requested transactions
//! upward to the coordinator but do not otherwise act on them.

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
};

use arrow_schema::Schema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    config::{InputEndpointConfig, OutputEndpointConfig},
    program_schema::SqlIdentifier,
    runtime_status::RuntimeDesiredStatus,
    suspend::TemporarySuspendError,
};

/// `/coordination/activate` request, sent by coordinator to pipeline to
/// transition out of [RuntimeDesiredStatus::Coordination].
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CoordinationActivate {
    /// The socket address that "exchange" operators should use to reach each of
    /// the hosts in the multihost pipeline.
    ///
    /// The `usize` component of each tuple is the number of workers at that
    /// socket address.
    pub exchanges: Vec<(SocketAddr, usize)>,

    /// The local address for this pipeline.  This must be one of the addresses
    /// in `exchanges`.
    pub local_address: SocketAddr,

    /// The desired status for the pipeline to initially enter.
    pub desired_status: RuntimeDesiredStatus,

    /// The checkpoint that the pipeline should start from, if any.
    pub checkpoint: Option<Uuid>,

    /// Local input endpoint configuration.
    pub inputs: BTreeMap<Cow<'static, str>, InputEndpointConfig>,

    /// Local output endpoint configuration.
    #[serde(default)]
    pub outputs: BTreeMap<Cow<'static, str>, OutputEndpointConfig>,
}

/// A step number.
pub type Step = u64;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum StepAction {
    /// Wait for instructions from the coordinator.
    Idle,
    /// Wait for a triggering event to occur, such as arrival of a sufficient
    /// amount of data on an input connector.  If one does, execute a step.
    Trigger,
    /// Execute a step.
    Step,
}

/// `/coordination/step/status` update, streamed by pipeline to coordinator.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StepStatus {
    /// The step that is running or will run next.
    pub step: Step,
    /// Current action.
    pub action: StepAction,
}

impl StepStatus {
    pub fn new(step: Step, action: StepAction) -> Self {
        Self { step, action }
    }
    pub fn is_triggered(&self, step: Step) -> bool {
        *self == Self::new(step, StepAction::Step) || self.is_idle(step + 1)
    }
    pub fn is_idle(&self, step: Step) -> bool {
        *self == Self::new(step, StepAction::Idle)
    }
}

/// `/coordination/step/request` request, sent by coordinator to pipeline to
/// control step behavior.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StepRequest {
    /// The action for the pipeline to take:
    ///
    /// - [Idle][]: Do not start a step.
    ///
    /// - [Trigger][]: Start a step if input arrives on an input endpoint.
    ///
    /// - [Step][]: Start a step.
    ///
    /// [Idle]: StepAction::Idle
    /// [Trigger]: StepAction::Trigger
    /// [Step]: StepAction::Step
    pub action: StepAction,

    /// The step to which `action` applies.
    ///
    /// `action` applies only if `step` is the pipeline's current step.
    /// Otherwise, the pipeline will not start a step.
    pub step: Step,

    /// Input endpoints to consider, based on the `action`:
    ///
    /// - [Idle][]: Unused.
    ///
    /// - [Trigger][]: Only input arriving on the specified endpoints can
    ///   trigger a step.  If a step is triggered, it will only use input from
    ///   the specified endpoints.
    ///
    /// - [Step][]: The step only uses input from the specified endpoints.
    ///
    /// [Idle]: StepAction::Idle
    /// [Trigger]: StepAction::Trigger
    /// [Step]: StepAction::Step
    pub input_connectors: Vec<String>,
}

impl StepRequest {
    pub fn new(step: Step, action: StepAction, input_connectors: Vec<String>) -> Self {
        Self {
            step,
            action,
            input_connectors,
        }
    }
    pub fn new_idle(step: Step) -> Self {
        Self::new(step, StepAction::Idle, Vec::new())
    }
}

/// `/coordination/checkpoint/status`, streamed by pipeline to coordinator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointCoordination {
    /// This pipeline can't checkpoint yet for the given reasons.  The
    /// coordinator can't do anything to help.
    Delayed(Vec<TemporarySuspendError>),

    /// This pipeline can't checkpoint yet for the given reasons.  The
    /// coordinator must run the pipeline for another step to help clear up the
    /// issue.
    Barriers(Vec<TemporarySuspendError>),

    /// This pipeline is ready to write a checkpoint.
    Ready,

    /// This pipeline is writing a checkpoint.
    InProgress,

    /// Checkpoint failed.
    Error(String),

    /// The checkpoint is complete.
    Done,
}

/// `/coordination/transaction/status` update, streamed by pipeline to coordinator.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TransactionCoordination {
    /// Whether a transaction is open:
    ///
    /// - `None`: No transaction open or committing.
    ///
    /// - `Some(true)`: Transaction is open.
    ///
    /// - `Some(false)`: Transaction is committing.  The coordinator needs to
    ///   execute at least one more step to commit it.
    ///
    /// Only the API can open and close a transaction when coordination is
    /// enabled.
    pub status: Option<bool>,

    /// Endpoints that want to join a transaction, with their optional labels.
    pub requests: HashMap<String, Option<String>>,
}

/// `/coordination/adhoc/catalog` reply.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdHocCatalog {
    pub tables: Vec<AdHocTable>,
}

/// One table in an [AdHocCatalog].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdHocTable {
    pub name: SqlIdentifier,
    pub materialized: bool,
    pub indexed: bool,
    pub schema: Schema,
}

/// `/coordination/adhoc/scan` request.
///
/// The reply is a stream of undelimited Arrow IPC record batches.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdHocScan {
    /// The step whose data is to be scanned.  The client must hold a lease on
    /// the step.
    pub step: Step,

    /// The worker within the step whose data is to be scanned.
    pub worker: usize,

    /// Table to scan.
    pub table: SqlIdentifier,

    /// Columnar projection.
    pub projection: Option<Vec<usize>>,
}
