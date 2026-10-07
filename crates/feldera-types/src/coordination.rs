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
//! A pipeline within a multihost pipeline starts up as follows:
//!
//! 1. It enters a special [RuntimeStatus::Coordination] state, in which it
//!    waits for instructions from the coordinator.  It reports its status, and
//!    the coordination features that it supports (see
//!    [CoordinationCapabilities]), at `/coordination/status`.
//!
//! 2. The coordinator sends a [CoordinationActivate] request.  It tells the
//!    pipeline the addresses of all of the hosts, the checkpoint to start
//!    from, the input and output connectors that this host runs, and, for
//!    distributed input connectors, how their input is divided among the hosts
//!    ([CoordinationActivate::input_distribution]) and the values that
//!    host 0 chose for them ([CoordinationActivate::input_choices]).
//!
//! 3. The pipeline builds its circuit and restores the checkpoint.  If the
//!    pipeline changed since the checkpoint and the change needs approval, it
//!    waits for the coordinator to forward `/approve`.  An `/approve` that
//!    arrives before activation is remembered, so a host that the coordinator
//!    activates late does not wait again.
//!
//! 4. The pipeline initializes its connectors, and makes the values that its
//!    distributed input connectors chose available at
//!    `/coordination/input/choices` (see [InputChoices]).
//!
//! 5. Unless it must bootstrap, the pipeline runs one step, which needs every
//!    other host to be at this stage too.  Then it reports that it is paused,
//!    or bootstrapping.
//!
//! The pipeline exchanges no data with the other hosts before stage 5, so it
//! can do stages 1 to 4 while the other hosts have not started yet.  The
//! coordinator relies on that when a distributed input connector needs one
//! host to choose a value for all of them, such as a Delta Lake table's
//! version: it activates host 0 alone, waits until
//! `/coordination/input/choices` on host 0 succeeds, and then activates the
//! other hosts with those values.
//! It cannot wait for host 0 to be paused, because host 0 cannot get there
//! until the other hosts reach stage 5.
//!
//! The coordinator's documentation lists the stages of startup in order.
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
    collections::{BTreeMap, HashMap, HashSet},
    net::SocketAddr,
};

use arrow_schema::Schema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    config::{InputEndpointConfig, OutputEndpointConfig},
    program_schema::SqlIdentifier,
    runtime_status::{ExtendedRuntimeStatus, ExtendedRuntimeStatusError, RuntimeDesiredStatus},
    suspend::TemporarySuspendError,
};

/// `/coordination/status` update, streamed by pipeline to coordinator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct CoordinationStatus {
    pub incarnation_uuid: Uuid,
    pub status: Result<ExtendedRuntimeStatus, ExtendedRuntimeStatusError>,

    /// Coordination features that the pipeline supports.
    ///
    /// A pipeline that predates this field supports none of them.
    #[serde(default)]
    pub capabilities: CoordinationCapabilities,
}

/// Coordination features that a pipeline supports.
///
/// The coordinator uses these to refuse a configuration that a pipeline would
/// misinterpret.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CoordinationCapabilities {
    /// The pipeline obeys [CoordinationActivate::input_distribution].
    ///
    /// A pipeline without this capability ignores
    /// [ConnectorConfig::distributed](crate::config::ConnectorConfig::distributed),
    /// so each host would read all of the input of a distributed connector.
    #[serde(default)]
    pub distributed_inputs: bool,

    /// The pipeline reports the values that its distributed input connectors
    /// chose at `/coordination/input/choices`, and it obeys
    /// [CoordinationActivate::input_choices].
    #[serde(default)]
    pub input_choices: bool,
}

impl CoordinationCapabilities {
    /// The capabilities of this version of the pipeline.
    pub const fn current() -> Self {
        Self {
            distributed_inputs: true,
            input_choices: true,
        }
    }
}

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

    /// Global assignment of output streams to workers.
    pub output_assignment: BTreeMap<String, usize>,

    /// The distributed input connectors, by name.
    ///
    /// Every host has each of these connectors in `inputs`, and each host
    /// reads a different part of the connector's input.  A connector that is
    /// not in this map reads all of its input on the one host that has it.
    #[serde(default)]
    pub input_distribution: BTreeMap<String, InputDistribution>,

    /// For distributed input connectors that need one value for all of their
    /// hosts before they read input, the value that host 0 chose.
    ///
    /// The coordinator activates host 0 first, reads its choices from
    /// `/coordination/input/choices` (see [InputChoices]), and then
    /// activates the other hosts with this map.  Host 0's own map is empty.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub input_choices: BTreeMap<String, JsonValue>,
}

/// `/coordination/input/choices` reply.
///
/// Maps from the name of each distributed input connector that needs a choice
/// (see `TransportConfig::needs_input_choice`) to the value that this host
/// chose for it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InputChoices {
    pub choices: BTreeMap<String, JsonValue>,
}

/// How the coordinator divides the input of a distributed input connector
/// among the hosts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InputDistribution {
    /// The ordinal of the connector's home host.
    ///
    /// This is the host that would have the connector if it were not
    /// distributed.  Unit 0 of the input goes to this host, unit 1 to the next
    /// host, and so on (see [InputShard::owner]).  Thus, a connector whose
    /// input has only one unit stays on its home host, and connectors whose
    /// inputs have few units go to different hosts.
    pub home: usize,
}

/// The part of a distributed input connector's input that one host reads.
///
/// A connector divides its input into numbered units, such as Kafka
/// partitions.  Each unit goes to exactly one host.
///
/// A connector can record its shard in its resume metadata, to detect a resume
/// on a host that reads a different shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputShard {
    /// The ordinal of this host, in `0..n_hosts`.
    host: usize,

    /// The number of hosts.
    n_hosts: usize,

    /// See [InputDistribution::home].  This is in `0..n_hosts`.
    home: usize,
}

impl InputShard {
    /// The only shard of a single-host pipeline.  It contains all of the
    /// input.
    pub const ALL: Self = Self {
        host: 0,
        n_hosts: 1,
        home: 0,
    };

    /// Returns the shard that host `host` of `n_hosts` reads under
    /// `distribution`, or an error if an ordinal is out of range.
    pub fn new(
        host: usize,
        n_hosts: usize,
        distribution: InputDistribution,
    ) -> Result<Self, String> {
        if host >= n_hosts {
            return Err(format!("host ordinal {host} is not less than {n_hosts}"));
        }
        if distribution.home >= n_hosts {
            return Err(format!(
                "home host ordinal {} is not less than {n_hosts}",
                distribution.home
            ));
        }
        Ok(Self {
            host,
            n_hosts,
            home: distribution.home,
        })
    }

    /// Returns the ordinal of this host.
    pub fn host(&self) -> usize {
        self.host
    }

    /// Returns true if this host chooses the values that all of the hosts use
    /// (see [CoordinationActivate::input_choices]).
    pub fn is_leader(&self) -> bool {
        self.host == 0
    }

    /// Returns true if this host is the connector's home host (see
    /// [InputDistribution::home]).  A connector that has work that cannot be
    /// divided, such as following a Delta table's log, does it on this host.
    pub fn is_home(&self) -> bool {
        self.host == self.home
    }

    /// Returns the ordinal of the host that reads `unit`.
    pub fn owner(&self, unit: u64) -> usize {
        let n_hosts = self.n_hosts as u64;
        ((unit % n_hosts + self.home as u64) % n_hosts) as usize
    }

    /// Returns true if this host reads `unit`.
    pub fn contains(&self, unit: u64) -> bool {
        self.owner(unit) == self.host
    }
}

/// A step number.
pub type Step = u64;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum StepAction {
    /// Wait for instructions from the coordinator.
    Idle,
    /// Wait for a triggering condition to occur, such as arrival of a
    /// sufficient amount of data on an input connector or if a transaction is
    /// committing.  If one does, execute a step.
    Trigger,
    /// Execute a step.
    Step,
}

/// `/coordination/step/status` update, streamed by pipeline to coordinator.
#[derive(Copy, Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct StepStatus {
    /// The step that is running or will run next.
    pub step: Step,
    /// Current action.
    pub action: StepAction,
    /// Whether a transaction is open:
    ///
    /// - `None`: No transaction open or committing.
    ///
    /// - `Some(true)`: Transaction is open.
    ///
    /// - `Some(false)`: Transaction is committing.  The coordinator needs to
    ///   execute at least one more step to commit it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_status: Option<bool>,
}

impl StepStatus {
    pub fn new(step: Step, action: StepAction, transaction_status: Option<bool>) -> Self {
        Self {
            step,
            action,
            transaction_status,
        }
    }
    pub fn is_triggered(&self, step: Step) -> bool {
        (self.step == step && self.action == StepAction::Step) || self.is_idle(step + 1)
    }
    pub fn is_idle(&self, step: Step) -> bool {
        self.step == step && self.action == StepAction::Idle
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
    /// - [Trigger][]: Start a step if input arrives on an input endpoint or if
    ///   a transaction is committing.
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

    /// Which input endpoints to use.
    ///
    /// This is not significant for [StepAction::Idle].
    pub inputs: StepInputs,
}

impl StepRequest {
    pub fn new(step: Step, action: StepAction, inputs: StepInputs) -> Self {
        Self {
            step,
            action,
            inputs,
        }
    }
    pub fn new_idle(step: Step) -> Self {
        Self::new(step, StepAction::Idle, StepInputs::All)
    }
}

/// The input endpoints to consider in a [StepRequest].
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum StepInputs {
    /// Consider input arriving on any input endpoint to trigger a step.  For
    /// executing a step, use input from all the input endpoints.
    All,

    /// Consider input arriving only on input endpoints that are blocking a
    /// checkpoint to trigger a step.  For executing a step, use input only from
    /// input endpoints that are blocking a checkpoint.
    CheckpointBarriers,

    /// Do not consider any input as triggering a step.  For executing a step,
    /// do not use any input.
    ///
    /// This is for committing a transaction.
    None,
}

/// `/coordination/checkpoint/status`, streamed by pipeline to coordinator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointCoordination {
    /// This pipeline can't checkpoint yet for the given reasons.  The problem
    /// is something other than the coordinator needing to step past a barrier
    /// or a transaction commit.
    Delayed(Vec<TemporarySuspendError>),

    /// This pipeline can't checkpoint yet for the given reasons.  The
    /// coordinator must run the pipeline for another step to help clear up the
    /// issue, either to step past a barrier or to complete a transaction
    /// commit.
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
    /// Endpoints that want to join a transaction, with their optional labels.
    pub requests: HashMap<String, Option<String>>,

    /// The endpoints in `requests` whose request every host makes, as the
    /// hosts of a distributed input connector do for a transaction that
    /// covers all of their input.
    ///
    /// A pipeline that predates this field reports none.
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    pub all_hosts: HashSet<String>,

    /// Endpoints that made a request that every host makes, and have
    /// withdrawn it, in the current transaction.  The pipeline reports these
    /// until the transaction commits.
    ///
    /// The coordinator keeps the transaction open until every host reports
    /// such an endpoint here, so that a host that finishes early does not
    /// commit the transaction before another host joins it.  Unlike a request,
    /// which appears and disappears, this stays until the commit, so the
    /// coordinator cannot miss it even if it does not see every update.
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    pub all_hosts_done: HashSet<String>,
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
    #[serde(default)]
    pub table_type: AdHocTableType,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdHocTableType {
    #[default]
    View,
    Table,
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

/// `/coordination/labels/incomplete` reply, streamed from pipeline to coordinator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Labels {
    /// All the labels on incomplete connectors.
    pub incomplete: HashSet<String>,
}

/// `/coordination/completion/status` reply, streamed from pipeline to coordinator.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Completion {
    /// Number of steps whose input records have been processed to completion.
    ///
    /// A record is processed to completion if it has been processed by the DBSP engine and
    /// all outputs derived from it have been processed by all output connectors.
    ///
    /// # Interpretation
    ///
    /// This is a count, not a step number.  If `total_completed_steps` is 0, no
    /// steps have been processed to completion.  If `total_completed_steps >
    /// 0`, then the last step whose input records have been processed to
    /// completion is `total_completed_steps - 1`. A record that was ingested
    /// in step `n` is fully processed when `total_completed_steps > n`.
    #[serde(rename = "c")]
    pub total_completed_steps: Step,
}

/// `/coordination/restart` arguments.
///
/// This pipeline request restarts the pipeline process.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestartArgs {
    /// The incarnation UUID of the pipeline process to restart.
    ///
    /// If this doesn't match the incarnation UUID of the running pipeline, then
    /// the request returns an error (since it has presumably already
    /// restarted).
    pub incarnation_uuid: Uuid,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::{
        CoordinationActivate, CoordinationCapabilities, CoordinationStatus, InputDistribution,
        InputShard,
    };

    fn shards(n_hosts: usize, home: usize) -> Vec<InputShard> {
        (0..n_hosts)
            .map(|host| InputShard::new(host, n_hosts, InputDistribution { home }).unwrap())
            .collect()
    }

    proptest! {
        /// Exactly one host reads each unit, and each host knows which one.
        #[test]
        fn each_unit_has_one_owner(
            (n_hosts, home) in (1usize..16).prop_flat_map(|n| (Just(n), 0..n)),
            units in prop::collection::vec(any::<u64>(), 0..64),
        ) {
            let shards = shards(n_hosts, home);
            for unit in units {
                let owner = shards[0].owner(unit);
                prop_assert!(owner < n_hosts);
                for shard in &shards {
                    prop_assert_eq!(shard.owner(unit), owner);
                    prop_assert_eq!(shard.contains(unit), shard.host == owner);
                }
            }
        }

        /// Consecutive units go to consecutive hosts, starting at the home
        /// host, so that `n` units occupy `min(n, n_hosts)` hosts.
        #[test]
        fn units_start_at_home(
            (n_hosts, home) in (1usize..16).prop_flat_map(|n| (Just(n), 0..n)),
            n_units in 0u64..40,
        ) {
            let shard = shards(n_hosts, home)[0];
            for unit in 0..n_units {
                prop_assert_eq!(shard.owner(unit), (home + unit as usize) % n_hosts);
            }
        }
    }

    #[test]
    fn single_host_reads_everything() {
        for unit in [0, 1, 7, u64::MAX] {
            assert!(InputShard::ALL.contains(unit));
        }
    }

    #[test]
    fn out_of_range_ordinals_are_rejected() {
        assert!(InputShard::new(2, 2, InputDistribution { home: 0 }).is_err());
        assert!(InputShard::new(0, 2, InputDistribution { home: 2 }).is_err());
        assert!(InputShard::new(0, 0, InputDistribution { home: 0 }).is_err());
        assert!(InputShard::new(1, 2, InputDistribution { home: 1 }).is_ok());
    }

    /// A coordinator or pipeline that predates the distribution fields must
    /// still interoperate: the missing fields mean "no distributed inputs".
    #[test]
    fn old_messages_have_no_distribution() {
        let activate: CoordinationActivate = serde_json::from_value(serde_json::json!({
            "exchanges": [],
            "local_address": "127.0.0.1:1",
            "desired_status": "Paused",
            "checkpoint": null,
            "inputs": {},
            "output_assignment": {},
        }))
        .unwrap();
        assert_eq!(activate.input_distribution, BTreeMap::new());

        let status: CoordinationStatus = serde_json::from_value(serde_json::json!({
            "incarnation_uuid": "00000000-0000-0000-0000-000000000000",
            "status": {"Err": {
                "status_code": 503,
                "error": {"message": "x", "error_code": "y", "details": null},
            }},
        }))
        .unwrap();
        assert_eq!(status.capabilities, CoordinationCapabilities::default());
        assert!(!status.capabilities.distributed_inputs);
        assert!(CoordinationCapabilities::current().distributed_inputs);
    }
}
