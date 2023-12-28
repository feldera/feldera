#[cfg(feature = "pg-embed")]
use crate::config::ApiServerConfig;
use crate::{
    auth::{TenantId, TenantRecord},
    compiler::ProgramStatus,
    config::DatabaseConfig,
};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use deadpool_postgres::{Manager, Pool, RecyclingMethod, Transaction};
use futures_util::TryFutureExt;
use log::{debug, info};
use openssl::sha;
use pipeline_types::config::{
    ConnectorConfig, InputEndpointConfig, OutputEndpointConfig, PipelineConfig, RuntimeConfig,
    ServiceConfig,
};
use pipeline_types::error::ErrorResponse;
use pipeline_types::query::OutputQuery;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashSet},
    fmt,
    fmt::Display,
    str::FromStr,
};
use storage::Storage;
use tokio_postgres::{error::Error as PgError, NoTls, Row};
use utoipa::ToSchema;
use uuid::Uuid;

#[cfg(test)]
pub(crate) mod test;

#[cfg(feature = "pg-embed")]
mod pg_setup;
pub(crate) mod storage;

mod error;
pub use error::DBError;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("./migrations/");
}

/// Project database API.
///
/// The API assumes that the caller holds a database lock, and therefore
/// doesn't use transactions (and hence doesn't need to deal with conflicts).
///
/// # Compilation queue
///
/// We use the `status` and `status_since` columns to maintain the compilation
/// queue.  A program is enqueued for compilation by setting its status to
/// `ProgramStatus::Pending`.  The `status_since` column is set to the current
/// time, which determines the position of the program in the queue.
pub struct ProjectDB {
    pub config: tokio_postgres::Config,
    pool: Pool,
    // Used in dev mode for having an embedded Postgres DB live through the
    // lifetime of the program.
    #[cfg(feature = "pg-embed")]
    #[allow(dead_code)] // It has to stay alive until ProjectDB is dropped.
    pg_inst: Option<pg_embed::postgres::PgEmbed>,
}

/// Unique pipeline id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct PipelineId(#[cfg_attr(test, proptest(strategy = "test::limited_uuid()"))] pub Uuid);
impl Display for PipelineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Unique connector id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct ConnectorId(#[cfg_attr(test, proptest(strategy = "test::limited_uuid()"))] pub Uuid);
impl Display for ConnectorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Unique attached connector id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct AttachedConnectorId(
    #[cfg_attr(test, proptest(strategy = "test::limited_uuid()"))] pub Uuid,
);
impl Display for AttachedConnectorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Unique service id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct ServiceId(#[cfg_attr(test, proptest(strategy = "test::limited_uuid()"))] pub Uuid);
impl Display for ServiceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Version number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct Version(#[cfg_attr(test, proptest(strategy = "1..3i64"))] pub i64);
impl Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Revision number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct Revision(Uuid);
impl Display for Revision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Pipeline status.
///
/// This type represents the state of the pipeline tracked by the pipeline
/// runner and observed by the API client via the `GET /pipeline` endpoint.
///
/// ### The lifecycle of a pipeline
///
/// The following automaton captures the lifecycle of the pipeline.  Individual
/// states and transitions of the automaton are described below.
///
/// * In addition to the transitions shown in the diagram, all states have an
///   implicit "forced shutdown" transition to the `Shutdown` state.  This
///   transition is triggered when the pipeline runner is unable to communicate
///   with the pipeline and thereby forces a shutdown.
///
/// * States labeled with the hourglass symbol (⌛) are **timed** states.  The
///   automaton stays in timed state until the corresponding operation completes
///   or until the runner performs a forced shutdown of the pipeline after a
///   pre-defined timeout perioud.
///
/// * State transitions labeled with API endpoint names (`/deploy`, `/start`,
///   `/pause`, `/shutdown`) are triggered by invoking corresponding endpoint,
///   e.g., `POST /v0/pipelines/{pipeline_id}/start`.
///
/// ```text
///                  Shutdown◄────┐
///                     │         │
///              /deploy│         │
///                     │   ⌛ShuttingDown
///                     ▼         ▲
///             ⌛Provisioning    │
///                     │         │
///  Provisioned        │         │
///                     ▼         │/shutdown
///             ⌛Initializing    │
///                     │         │
///            ┌────────┴─────────┴─┐
///            │        ▼           │
///            │      Paused        │
///            │      │    ▲        │
///            │/start│    │/pause  │
///            │      ▼    │        │
///            │     Running        │
///            └──────────┬─────────┘
///                       │
///                       ▼
///                     Failed
/// ```
///
/// ### Desired and actual status
///
/// We use the desired state model to manage the lifecycle of a pipeline.
/// In this model, the pipeline has two status attributes associated with
/// it at runtime: the **desired** status, which represents what the user
/// would like the pipeline to do, and the **current** status, which
/// represents the actual state of the pipeline.  The pipeline runner
/// service continuously monitors both fields and steers the pipeline
/// towards the desired state specified by the user.
// Using rustdoc references in the following paragraph upsets `docusaurus`.
/// Only three of the states in the pipeline automaton above can be
/// used as desired statuses: `Paused`, `Running`, and `Shutdown`.
/// These statuses are selected by invoking REST endpoints shown
/// in the diagram.
///
/// The user can monitor the current state of the pipeline via the
/// `/status` endpoint, which returns an object of type `Pipeline`.
/// In a typical scenario, the user first sets
/// the desired state, e.g., by invoking the `/deploy` endpoint, and
/// then polls the `GET /pipeline` endpoint to monitor the actual status
/// of the pipeline until its `state.current_status` attribute changes
/// to "paused" indicating that the pipeline has been successfully
/// initialized, or "failed", indicating an error.
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum PipelineStatus {
    /// Pipeline has not been started or has been shut down.
    ///
    /// The pipeline remains in this state until the user triggers
    /// a deployment by invoking the `/deploy` endpoint.
    Shutdown,

    /// The runner triggered a deployment of the pipeline and is
    /// waiting for the pipeline HTTP server to come up.
    ///
    /// In this state, the runner provisions a runtime for the pipeline
    /// (e.g., a Kubernetes pod or a local process), starts the pipeline
    /// within this runtime and waits for it to start accepting HTTP
    /// requests.
    ///
    /// The user is unable to communicate with the pipeline during this
    /// time.  The pipeline remains in this state until:
    ///
    /// 1. Its HTTP server is up and running; the pipeline transitions to the
    ///    [`Initializing`](`Self::Initializing`) state.
    /// 2. A pre-defined timeout has passed.  The runner performs forced
    ///    shutdown of the pipeline; returns to the
    ///    [`Shutdown`](`Self::Shutdown`) state.
    /// 3. The user cancels the pipeline by invoking the `/shutdown` endpoint.
    ///    The manager performs forced shutdown of the pipeline, returns to the
    ///    [`Shutdown`](`Self::Shutdown`) state.
    Provisioning,

    /// The pipeline is initializing its internal state and connectors.
    ///
    /// This state is part of the pipeline's deployment process.  In this state,
    /// the pipeline's HTTP server is up and running, but its query engine
    /// and input and output connectors are still initializing.
    ///
    /// The pipeline remains in this state until:
    ///
    /// 1. Intialization completes successfully; the pipeline transitions to the
    ///    [`Paused`](`Self::Paused`) state.
    /// 2. Intialization fails; transitions to the [`Failed`](`Self::Failed`)
    ///    state.
    /// 3. A pre-defined timeout has passed.  The runner performs forced
    ///    shutdown of the pipeline; returns to the
    ///    [`Shutdown`](`Self::Shutdown`) state.
    /// 4. The user cancels the pipeline by invoking the `/shutdown` endpoint.
    ///    The manager performs forced shutdown of the pipeline, returns to the
    ///    [`Shutdown`](`Self::Shutdown`) state.
    Initializing,

    /// The pipeline is fully initialized, but data processing has been paused.
    ///
    /// The pipeline remains in this state until:
    ///
    /// 1. The user starts the pipeline by invoking the `/start` endpoint. The
    ///    manager passes the request to the pipeline; transitions to the
    ///    [`Running`](`Self::Running`) state.
    /// 2. The user cancels the pipeline by invoking the `/shutdown` endpoint.
    ///    The manager passes the shutdown request to the pipeline to perform a
    ///    graceful shutdown; transitions to the
    ///    [`ShuttingDown`](`Self::ShuttingDown`) state.
    /// 3. An unexpected runtime error renders the pipeline
    ///    [`Failed`](`Self::Failed`).
    Paused,

    /// The pipeline is processing data.
    ///
    /// The pipeline remains in this state until:
    ///
    /// 1. The user pauses the pipeline by invoking the `/pause` endpoint. The
    ///    manager passes the request to the pipeline; transitions to the
    ///    [`Paused`](`Self::Paused`) state.
    /// 2. The user cancels the pipeline by invoking the `/shutdown` endpoint.
    ///    The runner passes the shutdown request to the pipeline to perform a
    ///    graceful shutdown; transitions to the
    ///    [`ShuttingDown`](`Self::ShuttingDown`) state.
    /// 3. An unexpected runtime error renders the pipeline
    ///    [`Failed`](`Self::Failed`).
    Running,

    /// Graceful shutdown in progress.
    ///
    /// In this state, the pipeline finishes any ongoing data processing,
    /// produces final outputs, shuts down input/output connectors and
    /// terminates.
    ///
    /// The pipeline remains in this state until:
    ///
    /// 1. Shutdown completes successfully; transitions to the
    ///    [`Shutdown`](`Self::Shutdown`) state.
    /// 2. A pre-defined timeout has passed.  The manager performs forced
    ///    shutdown of the pipeline; returns to the
    ///    [`Shutdown`](`Self::Shutdown`) state.
    ShuttingDown,

    /// The pipeline remains in this state until the users acknowledges the
    /// error by issuing a `/shutdown` request; transitions to the
    /// [`Shutdown`](`Self::Shutdown`) state.
    Failed,
}

impl TryFrom<String> for PipelineStatus {
    type Error = DBError;
    fn try_from(value: String) -> Result<Self, DBError> {
        match value.as_str() {
            "shutdown" => Ok(Self::Shutdown),
            "provisioning" => Ok(Self::Provisioning),
            "initializing" => Ok(Self::Initializing),
            "paused" => Ok(Self::Paused),
            "running" => Ok(Self::Running),
            "failed" => Ok(Self::Failed),
            "shutting_down" => Ok(Self::ShuttingDown),
            _ => Err(DBError::unknown_pipeline_status(value)),
        }
    }
}

impl From<PipelineStatus> for &'static str {
    fn from(val: PipelineStatus) -> Self {
        match val {
            PipelineStatus::Shutdown => "shutdown",
            PipelineStatus::Provisioning => "provisioning",
            PipelineStatus::Initializing => "initializing",
            PipelineStatus::Paused => "paused",
            PipelineStatus::Running => "running",
            PipelineStatus::Failed => "failed",
            PipelineStatus::ShuttingDown => "shutting_down",
        }
    }
}

/// A pipeline revision is a versioned, immutable configuration struct that
/// contains all information necessary to run a pipeline.
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone)]
pub struct PipelineRevision {
    /// The revision number, starts at 1, increases every time the pipeline is
    /// comitted.
    pub(crate) revision: Revision,
    /// The versioned pipeline descriptor.
    pub(crate) pipeline: PipelineDescr,
    /// The versioned connectors.
    pub(crate) connectors: Vec<ConnectorDescr>,
    /// The versioned program descriptor.
    pub(crate) program: ProgramDescr,
    /// The generated expanded configuration for the pipeline.
    pub(crate) config: PipelineConfig,
    // So new must be called if used outside of this module.
    #[serde(skip)]
    _private: (),
}

impl PipelineRevision {
    /// Create a new PipelineRevison.
    ///
    /// This is only ever invoked when reading a historical revision from
    /// history tables, which is why we expect it to be consistent
    pub(crate) fn new(
        revision: Revision,
        pipeline: PipelineDescr,
        connectors: Vec<ConnectorDescr>,
        program: ProgramDescr,
    ) -> Self {
        assert!(
            PipelineRevision::validate(&pipeline, &connectors, &program).is_ok(),
            "pre-condition: Validate supplied data is a consistent/valid snapshot"
        );
        // This unwrap() will succeed because the pre-conditions above make sure that
        // the config is valid
        let config = PipelineRevision::generate_pipeline_config(&pipeline, &connectors).unwrap();

        PipelineRevision {
            revision,
            pipeline,
            connectors,
            program,
            config,
            _private: (),
        }
    }

    /// Checks the invariants for a pipeline revision struct.
    ///
    /// They are:
    /// - The `pipeline.program_id` matches the `program.id` (and is not None).
    /// - The `program` is in Success status and has a schema.
    /// - The `connectors` vector contains all connectors referenced by the
    ///   attached connectors.
    /// - attached connectors only reference table/view names that exist in the
    ///   `program.schema`.
    fn validate(
        pipeline: &PipelineDescr,
        connectors: &[ConnectorDescr],
        program: &ProgramDescr,
    ) -> Result<(), DBError> {
        // The program was successfully compiled and has a schema
        if !program.status.is_compiled() || program.schema.is_none() {
            return Err(DBError::ProgramNotCompiled);
        }
        if program.status.has_failed_to_compile() {
            return Err(DBError::ProgramFailedToCompile);
        }
        // The pipline program_id is set
        if pipeline.program_name.is_none() {
            return Err(DBError::ProgramNotSet);
        }
        // ..  and matches the provided program name (this is an assert because
        // it's an error that can't be caused by a end-user)
        assert_eq!(
            pipeline.program_name.clone().unwrap(),
            program.name,
            "pre-condition: pipeline program and program_descr match"
        );
        // We supplied all connectors referenced by the `pipeline`, also an assert
        // (not possible to trigger by end-user)
        assert_eq!(
            HashSet::<String>::from_iter(
                pipeline
                    .attached_connectors
                    .iter()
                    .map(|ac| ac.connector_name.clone())
            ),
            HashSet::from_iter(connectors.iter().map(|c| c.name.clone())),
            "pre-condition: supplied all connectors necessary"
        );

        // This unwrap() is ok since we checked above that the program has a schema
        let schema = program.schema.as_ref().unwrap();

        let tables = HashSet::<_>::from_iter(schema.inputs.iter().map(|r| r.name.clone()));
        let acs_with_missing_tables: Vec<(String, String)> = pipeline
            .attached_connectors
            .iter()
            .filter(|ac| ac.is_input && !tables.contains(&ac.relation_name))
            .map(|ac| (ac.name.clone(), ac.relation_name.clone()))
            .collect();
        if !acs_with_missing_tables.is_empty() {
            return Err(DBError::TablesNotInSchema {
                missing: acs_with_missing_tables,
            });
        }

        let views = HashSet::<_>::from_iter(schema.outputs.iter().map(|r| r.name.clone()));
        let acs_with_missing_views: Vec<(String, String)> = pipeline
            .attached_connectors
            .iter()
            .filter(|ac| !ac.is_input && !views.contains(&ac.relation_name))
            .map(|ac| (ac.name.clone(), ac.relation_name.clone()))
            .collect();
        if !acs_with_missing_views.is_empty() {
            return Err(DBError::ViewsNotInSchema {
                missing: acs_with_missing_views,
            });
        }

        Ok(())
    }

    /// Generates a toml formatted config for the pipeline.
    ///
    /// Returns an error in case the config is invalid (e.g., a connector is
    /// missing during generation)
    pub(crate) fn generate_pipeline_config(
        pipeline: &PipelineDescr,
        connectors: &[ConnectorDescr],
    ) -> Result<PipelineConfig, DBError> {
        let pipeline_id = pipeline.pipeline_id;
        // input attached connectors
        let inputs: Vec<AttachedConnector> = pipeline
            .attached_connectors
            .iter()
            .filter(|ac| ac.is_input)
            .cloned()
            .collect();
        // output attached connectors
        let outputs: Vec<AttachedConnector> = pipeline
            .attached_connectors
            .iter()
            .filter(|ac| !ac.is_input)
            .cloned()
            .collect();

        // Expand input and output attached connectors
        let mut expanded_inputs: BTreeMap<Cow<'static, str>, InputEndpointConfig> = BTreeMap::new();
        for ac in inputs.iter() {
            let connector = connectors.iter().find(|c| ac.connector_name == c.name);
            if connector.is_none() {
                return Err(DBError::UnknownConnectorName {
                    connector_name: ac.connector_name.clone(),
                });
            }
            let input_endpoint_config = InputEndpointConfig {
                stream: Cow::from(ac.relation_name.clone()),
                connector_config: connector.unwrap().config.clone(),
            };
            expanded_inputs.insert(Cow::from(ac.name.clone()), input_endpoint_config);
        }
        let mut expanded_outputs: BTreeMap<Cow<'static, str>, OutputEndpointConfig> =
            BTreeMap::new();
        for ac in outputs.iter() {
            let connector = connectors.iter().find(|c| ac.connector_name == c.name);
            if connector.is_none() {
                return Err(DBError::UnknownConnectorName {
                    connector_name: ac.connector_name.clone(),
                });
            }
            let output_endpoint_config = OutputEndpointConfig {
                stream: Cow::from(ac.relation_name.clone()),
                // This field gets skipped during serialization/deserialization,
                // so it doesn't matter what value we use here
                query: OutputQuery::default(),
                connector_config: connector.unwrap().config.clone(),
            };
            expanded_outputs.insert(Cow::from(ac.name.clone()), output_endpoint_config);
        }

        let pc = PipelineConfig {
            name: Some(format!("pipeline-{pipeline_id}")),
            global: pipeline.config.clone(),
            inputs: expanded_inputs,
            outputs: expanded_outputs,
        };

        Ok(pc)
    }
}

/// Pipeline descriptor.
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone)]
pub(crate) struct PipelineDescr {
    pub pipeline_id: PipelineId,
    pub program_name: Option<String>,
    pub version: Version,
    pub name: String,
    pub description: String,
    pub config: RuntimeConfig,
    pub attached_connectors: Vec<AttachedConnector>,
}

/// Runtime state of the pipeine.
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) struct PipelineRuntimeState {
    /// Location where the pipeline can be reached at runtime.
    /// e.g., a TCP port number or a URI.
    pub location: String,

    /// Desired pipeline status, i.e., the status requested by the user.
    ///
    /// Possible values are [`Shutdown`](`PipelineStatus::Shutdown`),
    /// [`Paused`](`PipelineStatus::Paused`), and
    /// [`Running`](`PipelineStatus::Running`).
    pub desired_status: PipelineStatus,

    /// Current status of the pipeline.
    pub current_status: PipelineStatus,

    /// Time when the pipeline was assigned its current status
    /// of the pipeline.
    #[cfg_attr(test, proptest(value = "Utc::now()"))]
    pub status_since: DateTime<Utc>,

    /// Error that caused the pipeline to fail.
    ///
    /// This field is only used when the `current_status` of the pipeline
    /// is [`Shutdown`](`PipelineStatus::Shutdown`) or
    /// [`Failed`](`PipelineStatus::Failed`).
    /// When present, this field contains the error that caused
    /// the pipeline to terminate abnormally.
    // TODO: impl `Arbitrary` for `ErrorResponse`.
    #[cfg_attr(test, proptest(value = "None"))]
    pub error: Option<ErrorResponse>,

    /// Time when the pipeline started executing.
    #[cfg_attr(test, proptest(value = "Utc::now()"))]
    pub created: DateTime<Utc>,
}

impl PipelineRuntimeState {
    pub(crate) fn set_current_status(
        &mut self,
        new_current_status: PipelineStatus,
        error: Option<ErrorResponse>,
    ) {
        self.current_status = new_current_status;
        self.error = error;
        self.status_since = Utc::now();
    }

    pub(crate) fn set_location(&mut self, location: String) {
        self.location = location;
    }

    pub(crate) fn set_created(&mut self) {
        self.created = Utc::now();
    }
}

/// State of a pipeline, including static configuration
/// and runtime status.
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone)]
pub(crate) struct Pipeline {
    /// Static configuration of the pipeline.
    pub descriptor: PipelineDescr,

    /// Runtime state of the pipeline.
    pub state: PipelineRuntimeState,
}

/// Format to add attached connectors during a config update.
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) struct AttachedConnector {
    /// A unique identifier for this attachement.
    pub name: String,
    /// True for input connectors, false for output connectors.
    pub is_input: bool,
    /// The name of the connector to attach.
    pub connector_name: String,
    /// The table or view this connector is attached to. Unquoted
    /// table/view names in the SQL program need to be capitalized
    /// here. Quoted table/view names have to exactly match the
    /// casing from the SQL program.
    #[cfg_attr(test, proptest(regex = "relation1|relation2|relation3|"))]
    pub relation_name: String,
}

/// Connector descriptor.
#[derive(Deserialize, Serialize, ToSchema, Debug, Clone, Eq, PartialEq)]
pub(crate) struct ConnectorDescr {
    pub connector_id: ConnectorId,
    pub name: String,
    pub description: String,
    pub config: ConnectorConfig,
}

/// Service descriptor.
#[derive(Deserialize, Serialize, ToSchema, Debug, Clone, Eq, PartialEq)]
pub(crate) struct ServiceDescr {
    pub service_id: ServiceId,
    pub name: String,
    pub description: String,
    pub config: ServiceConfig,
}

/// ApiKey ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct ApiKeyId(#[cfg_attr(test, proptest(strategy = "test::limited_uuid()"))] pub Uuid);
impl Display for ApiKeyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// ApiKey descriptor.
#[derive(Deserialize, Serialize, ToSchema, Debug, Clone, Eq, PartialEq)]
pub(crate) struct ApiKeyDescr {
    pub id: ApiKeyId,
    pub name: String,
    pub scopes: Vec<ApiPermission>,
}

/// Permission types for invoking pipeline manager APIs
#[derive(Deserialize, Serialize, ToSchema, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) enum ApiPermission {
    Read,
    Write,
}

const API_PERMISSION_READ: &str = "read";
const API_PERMISSION_WRITE: &str = "write";

impl FromStr for ApiPermission {
    type Err = ();

    fn from_str(input: &str) -> Result<ApiPermission, Self::Err> {
        match input {
            API_PERMISSION_READ => Ok(ApiPermission::Read),
            API_PERMISSION_WRITE => Ok(ApiPermission::Write),
            _ => Err(()),
        }
    }
}

fn convert_bigint_to_time(created_secs: i64) -> Result<DateTime<Utc>, DBError> {
    let created_naive =
        NaiveDateTime::from_timestamp_millis(created_secs * 1000).ok_or_else(|| {
            DBError::invalid_data(format!(
                "Invalid timestamp in 'pipeline_runtime_state.created' column: {created_secs}"
            ))
        })?;

    Ok(DateTime::<Utc>::from_naive_utc_and_offset(
        created_naive,
        Utc,
    ))
}

// Re-exports
mod program;
pub(crate) use self::program::ColumnType;
pub(crate) use self::program::Field;
pub(crate) use self::program::ProgramDescr;
pub use self::program::ProgramId;
pub(crate) use self::program::ProgramSchema;
pub(crate) use self::program::Relation;

// The goal for these methods is to avoid multiple DB interactions as much as
// possible and if not, use transactions
#[async_trait]
impl Storage for ProjectDB {
    async fn list_programs(
        &self,
        tenant_id: TenantId,
        with_code: bool,
    ) -> Result<Vec<ProgramDescr>, DBError> {
        Ok(program::list_programs(self, tenant_id, with_code).await?)
    }

    async fn new_program(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        program_name: &str,
        program_description: &str,
        program_code: &str,
    ) -> Result<(ProgramId, Version), DBError> {
        Ok(program::new_program(
            self,
            tenant_id,
            id,
            program_name,
            program_description,
            program_code,
        )
        .await?)
    }

    /// Optionally update different fields of a program. This call
    /// also accepts an optional version to do guarded updates to the code.
    async fn update_program(
        &self,
        tenant_id: TenantId,
        program_id: ProgramId,
        program_name: &Option<String>,
        program_description: &Option<String>,
        program_code: &Option<String>,
        status: &Option<ProgramStatus>,
        schema: &Option<ProgramSchema>,
        guard: Option<Version>,
    ) -> Result<Version, DBError> {
        Ok(program::update_program(
            self,
            tenant_id,
            program_id,
            program_name,
            program_description,
            program_code,
            status,
            schema,
            guard,
        )
        .await?)
    }

    /// Retrieve program descriptor.
    ///
    /// Returns `None` if `program_id` is not found in the database.
    async fn get_program_by_id(
        &self,
        tenant_id: TenantId,
        program_id: ProgramId,
        with_code: bool,
    ) -> Result<ProgramDescr, DBError> {
        Ok(program::get_program_by_id(self, tenant_id, program_id, with_code).await?)
    }

    /// Lookup program by name.
    async fn get_program_by_name(
        &self,
        tenant_id: TenantId,
        program_name: &str,
        with_code: bool,
        txn: Option<&Transaction<'_>>,
    ) -> Result<ProgramDescr, DBError> {
        Ok(program::get_program_by_name(self, tenant_id, program_name, with_code, txn).await?)
    }

    async fn delete_program(
        &self,
        tenant_id: TenantId,
        program_id: ProgramId,
    ) -> Result<(), DBError> {
        Ok(program::delete_program(self, tenant_id, program_id).await?)
    }

    async fn all_programs(&self) -> Result<Vec<(TenantId, ProgramDescr)>, DBError> {
        Ok(program::all_programs(self).await?)
    }

    async fn all_pipelines(&self) -> Result<Vec<(TenantId, PipelineId)>, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                r#"SELECT tenant_id, id
                FROM pipeline"#,
            )
            .await?;
        let rows = manager.query(&stmt, &[]).await?;

        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let tenant_id = TenantId(row.get(0));
            let pipeline_id = PipelineId(row.get(1));
            result.push((tenant_id, pipeline_id));
        }
        Ok(result)
    }

    async fn next_job(&self) -> Result<Option<(TenantId, ProgramId, Version)>, DBError> {
        let manager = self.pool.get().await?;
        // Find the oldest pending project.
        let stmt = manager
            .prepare_cached(
                "SELECT id, version, tenant_id FROM program WHERE status = 'pending' AND status_since = (SELECT min(status_since) FROM program WHERE status = 'pending')"
            )
            .await?;
        let res = manager.query(&stmt, &[]).await?;

        if let Some(row) = res.get(0) {
            let program_id: ProgramId = ProgramId(row.get(0));
            let version: Version = Version(row.get(1));
            let tenant_id: TenantId = TenantId(row.get(2));
            Ok(Some((tenant_id, program_id, version)))
        } else {
            Ok(None)
        }
    }

    /// Resolve all references and deploy a new pipeline
    async fn create_pipeline_revision(
        &self,
        deployment_id: Uuid,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<Revision, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let pipeline = self
            .get_pipeline_descr_by_id(tenant_id, pipeline_id, Some(&txn))
            .await?;
        let program_name = pipeline
            .program_name
            .as_ref()
            .ok_or(DBError::ProgramNotSet)?;
        let program = self
            .get_program_by_name(tenant_id, program_name, false, Some(&txn))
            .await?;
        let connectors = self
            .get_connectors_for_pipeline_id(tenant_id, pipeline_id, Some(&txn))
            .await?;
        PipelineRevision::validate(&pipeline, &connectors, &program)?;
        let pipeline_revision =
            PipelineRevision::new(Revision(deployment_id), pipeline, connectors, program);

        let pipeline_config = serde_json::to_string(&pipeline_revision).unwrap();
        let check_revision = txn
            .prepare_cached(
                "SELECT config FROM deployed_pipeline WHERE pipeline_id = $1 AND tenant_id = $2",
            )
            .await?;
        let row: Option<Row> = txn
            .query_opt(&check_revision, &[&pipeline_id.0, &tenant_id.0])
            .await?;

        if let Some(row) = row {
            let current_revision: String = row.get(0);
            if pipeline_config == current_revision {
                return Err(DBError::RevisionNotChanged);
            }
        }
        let stmt = txn
            .prepare_cached("INSERT INTO deployed_pipeline (id, pipeline_id, tenant_id, config) VALUES ($1, $2, $3, $4)
                             ON CONFLICT ON CONSTRAINT unique_pipeline_id 
                             DO UPDATE SET id = EXCLUDED.id, config = EXCLUDED.config")
            .await?;
        txn.execute(
            &stmt,
            &[
                &deployment_id,
                &pipeline_id.0,
                &tenant_id.0,
                &pipeline_config,
            ],
        )
        .await?;
        txn.commit().await?;
        Ok(Revision(deployment_id))
    }

    async fn get_current_pipeline_revision(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<PipelineRevision, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT config FROM deployed_pipeline WHERE pipeline_id = $1 AND tenant_id = $2",
            )
            .await?;
        let row: Option<Row> = manager
            .query_opt(&stmt, &[&pipeline_id.0, &tenant_id.0])
            .await?;

        if let Some(row) = row {
            let config: String = row.get(0);
            let pipeline_revision = serde_json::from_str(&config).unwrap();
            Ok(pipeline_revision)
        } else {
            // Err(DBError::UnknownPipeline { pipeline_id })
            Err(DBError::NoRevisionAvailable { pipeline_id })
        }
    }

    async fn list_pipelines(&self, tenant_id: TenantId) -> Result<Vec<Pipeline>, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT p.id, p.version, p.name, p.description, p.config, program.name,
            COALESCE(json_agg(json_build_object('name', ac.name,
                                                'connector_name', c.name,
                                                'config', ac.config,
                                                'is_input', is_input))
                            FILTER (WHERE ac.name IS NOT NULL),
                    '[]'),
            rt.location, rt.desired_status, rt.current_status, rt.status_since, rt.error, rt.created
            FROM pipeline p
            INNER JOIN pipeline_runtime_state rt on p.id = rt.id
            LEFT JOIN program on p.program_id = program.id
            LEFT JOIN attached_connector ac on p.id = ac.pipeline_id
            LEFT JOIN connector c on ac.connector_id = c.id
            WHERE p.tenant_id = $1
            GROUP BY p.id, rt.id, program.name;",
            )
            .await?;

        let rows: Vec<Row> = manager
            // For every pipeline, produce a JSON representation of all connectors
            .query(&stmt, &[&tenant_id.0])
            .await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            result.push(self.row_to_pipeline(&row).await?);
        }

        Ok(result)
    }

    async fn get_pipeline_by_id(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<Pipeline, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT p.id, p.version, p.name as cname, p.description, p.config, program.name,
                COALESCE(json_agg(json_build_object('name', ac.name,
                                                    'connector_name', c.name,
                                                    'config', ac.config,
                                                    'is_input', is_input))
                                FILTER (WHERE ac.name IS NOT NULL),
                        '[]'),
                rt.location, rt.desired_status, rt.current_status, rt.status_since, rt.error, rt.created
                FROM pipeline p
                INNER JOIN pipeline_runtime_state rt on p.id = rt.id
                LEFT JOIN program on p.program_id = program.id
                LEFT JOIN attached_connector ac on p.id = ac.pipeline_id
                LEFT JOIN connector c on ac.connector_id = c.id
                WHERE p.id = $1 AND p.tenant_id = $2
                GROUP BY p.id, rt.id, program.name
                ",
            )
            .await?;

        let row = manager
            .query_opt(&stmt, &[&pipeline_id.0, &tenant_id.0])
            .await?
            .ok_or(DBError::UnknownPipeline { pipeline_id })?;

        self.row_to_pipeline(&row).await
    }

    async fn get_pipeline_descr_by_id(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        txn: Option<&Transaction<'_>>,
    ) -> Result<PipelineDescr, DBError> {
        let query =
            "SELECT p.id, p.version, p.name as cname, p.description, p.config, program.name,
                COALESCE(json_agg(json_build_object('name', ac.name,
                                                    'connector_name', c.name,
                                                    'config', ac.config,
                                                    'is_input', is_input))
                                FILTER (WHERE ac.name IS NOT NULL),
                        '[]')
                FROM pipeline p
                LEFT JOIN program on p.program_id = program.id
                LEFT JOIN attached_connector ac on p.id = ac.pipeline_id
                LEFT JOIN connector c on ac.connector_id = c.id
                WHERE p.id = $1 AND p.tenant_id = $2
                GROUP BY p.id, program.name
                ";
        let row = if let Some(txn) = txn {
            let stmt = txn.prepare_cached(query).await?;
            txn.query_opt(&stmt, &[&pipeline_id.0, &tenant_id.0])
                .await?
                .ok_or(DBError::UnknownPipeline { pipeline_id })?
        } else {
            let manager = self.pool.get().await?;
            let stmt = manager.prepare_cached(query).await?;

            manager
                .query_opt(&stmt, &[&pipeline_id.0, &tenant_id.0])
                .await?
                .ok_or(DBError::UnknownPipeline { pipeline_id })?
        };

        self.row_to_pipeline_descr(&row).await
    }

    async fn get_pipeline_runtime_state(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<PipelineRuntimeState, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT location, desired_status, current_status, status_since, error, created
                FROM pipeline_runtime_state
                WHERE id = $1 AND tenant_id = $2",
            )
            .await?;

        let row = manager
            .query_opt(&stmt, &[&pipeline_id.0, &tenant_id.0])
            .await?;

        self.row_to_pipeline_runtime_state(pipeline_id, &row).await
    }

    async fn get_pipeline_descr_by_name(
        &self,
        tenant_id: TenantId,
        name: String,
    ) -> Result<PipelineDescr, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT p.id, p.version, p.name as cname, p.description, p.config, prog.name,
                COALESCE(json_agg(json_build_object('name', ac.name,
                                                    'connector_name', c.name,
                                                    'config', ac.config,
                                                    'is_input', is_input))
                                FILTER (WHERE ac.name IS NOT NULL),
                        '[]')
                FROM pipeline p
                LEFT JOIN program prog on p.program_id = prog.id
                LEFT JOIN attached_connector ac on p.id = ac.pipeline_id
                LEFT JOIN connector c on ac.connector_id = c.id
                WHERE p.name = $1 AND p.tenant_id = $2
                GROUP BY p.id, prog.name
                ",
            )
            .await?;

        let row = manager
            .query_opt(&stmt, &[&name, &tenant_id.0])
            .await?
            .ok_or(DBError::UnknownName { name })?;

        self.row_to_pipeline_descr(&row).await
    }

    async fn get_pipeline_by_name(
        &self,
        tenant_id: TenantId,
        name: String,
    ) -> Result<Pipeline, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT p.id, p.version, p.name as cname, p.description, p.config, program.name,
                COALESCE(json_agg(json_build_object('name', ac.name,
                                                    'connector_name', c.name,
                                                    'config', ac.config,
                                                    'is_input', is_input))
                                FILTER (WHERE ac.name IS NOT NULL),
                        '[]'),
                rt.location, rt.desired_status, rt.current_status, rt.status_since, rt.error, rt.created
                FROM pipeline p
                INNER JOIN pipeline_runtime_state rt on p.id = rt.id
                LEFT JOIN program on p.program_id = program.id
                LEFT JOIN attached_connector ac on p.id = ac.pipeline_id
                LEFT JOIN connector c on ac.connector_id = c.id
                WHERE p.name = $1 AND p.tenant_id = $2
                GROUP BY p.id, rt.id, program.name
                ",
            )
            .await?;

        let row = manager
            .query_opt(&stmt, &[&name, &tenant_id.0])
            .await?
            .ok_or(DBError::UnknownName { name })?;

        self.row_to_pipeline(&row).await
    }

    // XXX: Multiple statements
    async fn new_pipeline(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        program_name: &Option<String>,
        pipline_name: &str,
        pipeline_description: &str,
        config: &RuntimeConfig,
        connectors: &Option<Vec<AttachedConnector>>,
    ) -> Result<(PipelineId, Version), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let new_pipeline = txn
            .prepare_cached(
                "INSERT INTO pipeline (id, program_id, version, name, description, config, tenant_id) 
                 VALUES($1, $2, 1, $3, $4, $5, $6)")
            .await?;
        let new_runtime_state = txn
            .prepare_cached(
                "INSERT INTO pipeline_runtime_state (id, tenant_id, desired_status, current_status, status_since, created) VALUES($1, $2, 'shutdown', 'shutdown', extract(epoch from now()), extract(epoch from now()))")
            .await?;
        let program_id = if let Some(program_name) = program_name {
            let get_program_id = txn
                .prepare_cached("SELECT id FROM program WHERE tenant_id = $1 AND name = $2")
                .await?;
            txn.query_opt(&get_program_id, &[&tenant_id.0, &program_name])
                .await?
                .map(|r| Some(ProgramId(r.get(0))))
                .ok_or(DBError::UnknownProgramName {
                    program_name: program_name.to_string(),
                })?
        } else {
            None
        };
        let config_str = RuntimeConfig::to_yaml(config);
        txn.execute(
            &new_pipeline,
            &[
                &id,
                &program_id.map(|id| id.0),
                &pipline_name,
                &pipeline_description,
                &config_str,
                &tenant_id.0,
            ],
        )
        .await
        .map_err(ProjectDB::maybe_unique_violation)
        .map_err(|e| {
            ProjectDB::maybe_tenant_id_foreign_key_constraint_err(
                e,
                tenant_id,
                program_id.map(|e| e.0),
            )
        })
        .map_err(|e| {
            ProjectDB::maybe_program_id_not_found_foreign_key_constraint_err(e, program_id)
        })?;

        txn.execute(&new_runtime_state, &[&id, &tenant_id.0])
            .await?;

        let pipeline_id = PipelineId(id);

        if let Some(connectors) = connectors {
            // Add the connectors.
            for ac in connectors {
                self.attach_connector(tenant_id, &txn, pipeline_id, ac)
                    .await?;
            }
        }
        txn.commit().await?;

        Ok((pipeline_id, Version(1)))
    }

    // XXX: Multiple statements
    async fn update_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_name: &Option<String>,
        pipline_name: &str,
        pipeline_description: &str,
        config: &Option<RuntimeConfig>,
        connectors: &Option<Vec<AttachedConnector>>,
    ) -> Result<Version, DBError> {
        log::trace!(
            "Updating config {} {:?} {} {} {:?} {:?}",
            pipeline_id.0,
            program_name,
            pipline_name,
            pipeline_description,
            config,
            connectors
        );
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let find_pipeline_id = txn
            .prepare_cached("SELECT id FROM pipeline WHERE id = $1 AND tenant_id = $2")
            .await?;
        let get_program_id = txn
            .prepare_cached("SELECT id FROM program WHERE tenant_id = $1 AND name = $2")
            .await?;
        let delete_ac = txn
            .prepare_cached(
                "DELETE FROM attached_connector WHERE pipeline_id = $1 AND tenant_id = $2",
            )
            .await?;
        let update_pipeline = txn
            .prepare_cached(
                "UPDATE pipeline SET version = version + 1, name = $1, description = $2, config = COALESCE($3, config), program_id = $4 WHERE id = $5 AND tenant_id = $6 RETURNING version",
            )
            .await?;

        // First check whether the pipeline exists. Without this check, subsequent
        // calls will fail.
        let row = txn
            .query_opt(&find_pipeline_id, &[&pipeline_id.0, &tenant_id.0])
            .await?;
        if row.is_none() {
            return Err(DBError::UnknownPipeline { pipeline_id });
        }
        // Next, check if the program exists
        let program_id = if let Some(program_name) = program_name {
            txn.query_opt(&get_program_id, &[&tenant_id.0, &program_name])
                .await?
                .map(|r| Some(ProgramId(r.get(0))))
                .ok_or(DBError::UnknownProgramName {
                    program_name: program_name.to_string(),
                })?
        } else {
            None
        };
        if let Some(connectors) = connectors {
            // Delete all existing attached connectors.
            txn.execute(&delete_ac, &[&pipeline_id.0, &tenant_id.0])
                .await?;

            // Rewrite the new set of connectors.
            for ac in connectors {
                self.attach_connector(tenant_id, &txn, pipeline_id, ac)
                    .await?;
            }
        }
        let config = config.as_ref().map(RuntimeConfig::to_yaml);
        let row = txn
            .query_opt(
                &update_pipeline,
                &[
                    &pipline_name,
                    &pipeline_description,
                    &config,
                    &program_id.map(|id| id.0),
                    &pipeline_id.0,
                    &tenant_id.0,
                ],
            )
            .await
            .map_err(ProjectDB::maybe_unique_violation)
            .map_err(|e| {
                ProjectDB::maybe_program_id_not_found_foreign_key_constraint_err(e, program_id)
            })?;
        txn.commit().await?;
        match row {
            Some(row) => Ok(Version(row.get(0))),
            None => Err(DBError::UnknownPipeline { pipeline_id }),
        }
    }

    /// Update the runtime state of the pipeline.
    ///
    /// This function is meant for use by the runner, and therefore doesn't
    /// modify the `desired_status` column, which is always managed by the
    /// client.
    async fn update_pipeline_runtime_state(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        state: &PipelineRuntimeState,
    ) -> Result<(), DBError> {
        let current_status: &'static str = state.current_status.into();
        let manager = self.pool.get().await?;
        let update_runtime_state = manager
            .prepare_cached(
                "UPDATE pipeline_runtime_state
                SET location = $3,
                    current_status = $4,
                    status_since = $5,
                    created = $6,
                    error = $7
                WHERE id = $1 AND tenant_id = $2
                ",
            )
            .await?;

        let modified_rows = manager
            .execute(
                &update_runtime_state,
                &[
                    &pipeline_id.0,
                    &tenant_id.0,
                    &state.location,
                    &current_status,
                    &state.status_since.timestamp(),
                    &state.created.timestamp(),
                    &state
                        .error
                        .as_ref()
                        .map(|e| serde_json::to_string(&e).unwrap()),
                ],
            )
            .await?;

        if modified_rows == 0 {
            return Err(DBError::UnknownPipeline { pipeline_id });
        }
        Ok(())
    }

    /// Update the `desired_status` column.
    async fn set_pipeline_desired_status(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        desired_status: PipelineStatus,
    ) -> Result<(), DBError> {
        let desired_status: &'static str = desired_status.into();
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "UPDATE pipeline_runtime_state
                SET desired_status = $3
                WHERE tenant_id = $1 AND id = $2
                ",
            )
            .await?;

        let modified_rows = manager
            .execute(&stmt, &[&tenant_id.0, &pipeline_id.0, &desired_status])
            .await?;

        if modified_rows == 0 {
            return Err(DBError::UnknownPipeline { pipeline_id });
        }
        Ok(())
    }

    async fn delete_config(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<(), DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("DELETE FROM pipeline WHERE id = $1 AND tenant_id = $2")
            .await?;
        let res = manager
            .execute(&stmt, &[&pipeline_id.0, &tenant_id.0])
            .await?;
        if res > 0 {
            Ok(())
        } else {
            Err(DBError::UnknownPipeline { pipeline_id })
        }
    }

    /// Returns true if the connector of a given name is an input connector.
    async fn attached_connector_is_input(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        name: &str,
    ) -> Result<bool, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("SELECT is_input FROM attached_connector WHERE name = $1 AND pipeline_id = $2 AND tenant_id = $3")
            .await?;
        let row = manager
            .query_opt(&stmt, &[&name, &pipeline_id.0, &tenant_id.0])
            .await?;

        match row {
            Some(row) => Ok(row.get(0)),
            None => Err(DBError::UnknownAttachedConnector {
                pipeline_id,
                name: name.to_string(),
            }),
        }
    }

    async fn delete_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<bool, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("DELETE FROM pipeline WHERE id = $1 AND tenant_id = $2")
            .await?;
        let res = manager
            .execute(&stmt, &[&pipeline_id.0, &tenant_id.0])
            .await?;
        Ok(res > 0)
    }

    async fn new_connector(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        name: &str,
        description: &str,
        config: &ConnectorConfig,
    ) -> Result<ConnectorId, DBError> {
        debug!("new_connector {name} {description} {config:?}");
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("INSERT INTO connector (id, name, description, config, tenant_id) VALUES($1, $2, $3, $4, $5)")
            .await?;
        manager
            .execute(
                &stmt,
                &[&id, &name, &description, &config.to_yaml(), &tenant_id.0],
            )
            .await
            .map_err(ProjectDB::maybe_unique_violation)
            .map_err(|e| {
                ProjectDB::maybe_tenant_id_foreign_key_constraint_err(e, tenant_id, None)
            })?;
        Ok(ConnectorId(id))
    }

    async fn list_connectors(&self, tenant_id: TenantId) -> Result<Vec<ConnectorDescr>, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT id, name, description, config FROM connector WHERE tenant_id = $1",
            )
            .await?;
        let rows = manager.query(&stmt, &[&tenant_id.0]).await?;

        let mut result = Vec::with_capacity(rows.len());

        for row in rows {
            result.push(ConnectorDescr {
                connector_id: ConnectorId(row.get(0)),
                name: row.get(1),
                description: row.get(2),
                config: ConnectorConfig::from_yaml_str(row.get(3)),
            });
        }

        Ok(result)
    }

    async fn get_connector_by_name(
        &self,
        tenant_id: TenantId,
        name: String,
    ) -> Result<ConnectorDescr, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT id, description, config FROM connector WHERE name = $1 AND tenant_id = $2",
            )
            .await?;
        let row = manager.query_opt(&stmt, &[&name, &tenant_id.0]).await?;

        if let Some(row) = row {
            let connector_id: ConnectorId = ConnectorId(row.get(0));
            let description: String = row.get(1);
            let config = ConnectorConfig::from_yaml_str(row.get(2));

            Ok(ConnectorDescr {
                connector_id,
                name,
                description,
                config,
            })
        } else {
            Err(DBError::UnknownName { name })
        }
    }

    async fn get_connector_by_id(
        &self,
        tenant_id: TenantId,
        connector_id: ConnectorId,
    ) -> Result<ConnectorDescr, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT name, description, config FROM connector WHERE id = $1 AND tenant_id = $2",
            )
            .await?;

        let row = manager
            .query_opt(&stmt, &[&connector_id.0, &tenant_id.0])
            .await?;

        if let Some(row) = row {
            let name: String = row.get(0);
            let description: String = row.get(1);
            let config: String = row.get(2);
            let config = ConnectorConfig::from_yaml_str(&config);

            Ok(ConnectorDescr {
                connector_id,
                name,
                description,
                config,
            })
        } else {
            Err(DBError::UnknownConnector { connector_id })
        }
    }

    async fn update_connector(
        &self,
        tenant_id: TenantId,
        connector_id: ConnectorId,
        connector_name: &str,
        description: &str,
        config: &Option<ConnectorConfig>,
    ) -> Result<(), DBError> {
        let descr = self.get_connector_by_id(tenant_id, connector_id).await?;
        let config = config.clone().unwrap_or(descr.config);
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "UPDATE connector SET name = $1, description = $2, config = $3 WHERE id = $4",
            )
            .await?;

        manager
            .execute(
                &stmt,
                &[
                    &connector_name,
                    &description,
                    &config.to_yaml(),
                    &connector_id.0,
                ],
            )
            .await
            .map_err(Self::maybe_unique_violation)?;

        Ok(())
    }

    async fn delete_connector(
        &self,
        tenant_id: TenantId,
        connector_id: ConnectorId,
    ) -> Result<(), DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("DELETE FROM connector WHERE id = $1 AND tenant_id = $2")
            .await?;
        let res = manager
            .execute(&stmt, &[&connector_id.0, &tenant_id.0])
            .await?;

        if res > 0 {
            Ok(())
        } else {
            Err(DBError::UnknownConnector { connector_id })
        }
    }

    async fn list_api_keys(&self, tenant_id: TenantId) -> Result<Vec<ApiKeyDescr>, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("SELECT id, name, scopes FROM api_key WHERE tenant_id = $1")
            .await?;
        let rows = manager.query(&stmt, &[&tenant_id.0]).await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let id: ApiKeyId = ApiKeyId(row.get(0));
            let name: String = row.get(1);
            let vec: Vec<String> = row.get(2);
            let scopes = vec
                .iter()
                .map(|s| {
                    ApiPermission::from_str(s).expect("Unexpected ApiPermission string in the DB")
                })
                .collect();
            result.push(ApiKeyDescr { id, name, scopes });
        }
        Ok(result)
    }

    async fn get_api_key(&self, tenant_id: TenantId, name: &str) -> Result<ApiKeyDescr, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT id, name, scopes FROM api_key WHERE tenant_id = $1 and name = $2",
            )
            .await?;
        let maybe_row = manager.query_opt(&stmt, &[&tenant_id.0, &name]).await?;
        if let Some(row) = maybe_row {
            let id: ApiKeyId = ApiKeyId(row.get(0));
            let name: String = row.get(1);
            let vec: Vec<String> = row.get(2);
            let scopes = vec
                .iter()
                .map(|s| {
                    ApiPermission::from_str(s).expect("Unexpected ApiPermission string in the DB")
                })
                .collect();

            Ok(ApiKeyDescr { id, name, scopes })
        } else {
            Err(DBError::UnknownApiKey {
                name: name.to_string(),
            })
        }
    }

    async fn delete_api_key(&self, tenant_id: TenantId, name: &str) -> Result<(), DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("DELETE FROM api_key WHERE tenant_id = $1 AND name = $2")
            .await?;
        let res = manager.execute(&stmt, &[&tenant_id.0, &name]).await?;
        if res > 0 {
            Ok(())
        } else {
            Err(DBError::UnknownApiKey {
                name: name.to_string(),
            })
        }
    }

    async fn store_api_key_hash(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        name: &str,
        key: &str,
        scopes: Vec<ApiPermission>,
    ) -> Result<(), DBError> {
        let mut hasher = sha::Sha256::new();
        hasher.update(key.as_bytes());
        let hash = openssl::base64::encode_block(&hasher.finish());
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "INSERT INTO api_key (id, tenant_id, name, hash, scopes) VALUES ($1, $2, $3, $4, $5)",
            )
            .await?;
        let res = manager
            .execute(
                &stmt,
                &[
                    &id,
                    &tenant_id.0,
                    &name,
                    &hash,
                    &scopes
                        .iter()
                        .map(|scope| match scope {
                            ApiPermission::Read => API_PERMISSION_READ,
                            ApiPermission::Write => API_PERMISSION_WRITE,
                        })
                        .collect::<Vec<&str>>(),
                ],
            )
            .await
            .map_err(Self::maybe_unique_violation)
            .map_err(|e| {
                ProjectDB::maybe_tenant_id_foreign_key_constraint_err(e, tenant_id, None)
            })?;
        if res > 0 {
            Ok(())
        } else {
            Err(DBError::duplicate_key())
        }
    }

    async fn validate_api_key(
        &self,
        api_key: &str,
    ) -> Result<(TenantId, Vec<ApiPermission>), DBError> {
        let mut hasher = sha::Sha256::new();
        hasher.update(api_key.as_bytes());
        let hash = openssl::base64::encode_block(&hasher.finish());
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("SELECT tenant_id, scopes FROM api_key WHERE hash = $1")
            .await?;
        let res = manager
            .query_one(&stmt, &[&hash])
            .await
            .map_err(|_| DBError::InvalidKey)?;
        let tenant_id = TenantId(res.get(0));
        let vec: Vec<String> = res.get(1);
        let vec = vec
            .iter()
            .map(|s| ApiPermission::from_str(s).expect("Unexpected ApiPermission string in the DB"))
            .collect();
        Ok((tenant_id, vec))
    }

    async fn get_or_create_tenant_id(
        &self,
        tenant_name: String,
        provider: String,
    ) -> Result<TenantId, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("SELECT id FROM tenant WHERE tenant = $1 AND provider = $2")
            .await?;

        let res = manager.query_opt(&stmt, &[&tenant_name, &provider]).await?;
        match res {
            Some(row) => Ok(TenantId(row.get(0))),
            None => {
                self.create_tenant_if_not_exists(Uuid::now_v7(), tenant_name, provider)
                    .await
            }
        }
    }

    async fn create_tenant_if_not_exists(
        &self,
        tenant_id: Uuid,
        tenant_name: String,
        provider: String,
    ) -> Result<TenantId, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("INSERT INTO tenant (id, tenant, provider) VALUES ($1, $2, $3) ON CONFLICT (tenant, provider) DO UPDATE SET tenant = excluded.tenant, provider = excluded.provider RETURNING id")
            .await?;

        // Unfortunately, doing a read-if-exists-else-insert is not very ergonomic. To
        // do so in a single query requires us to do a redundant UPDATE on conflict,
        // where we set the tenant and provider values to their existing values
        // (excluded.{tenant, provider})
        let res = manager
            .query_one(&stmt, &[&tenant_id, &tenant_name, &provider])
            .await?;
        Ok(TenantId(res.get(0)))
    }

    async fn create_compiled_binary_ref(
        &self,
        program_id: ProgramId,
        version: Version,
        url: String,
    ) -> Result<(), DBError> {
        let conn = self.pool.get().await?;
        let stmt = conn
            .prepare_cached("INSERT INTO compiled_binary VALUES ($1, $2, $3)")
            .await?;

        let _res = conn
            .execute(&stmt, &[&program_id.0, &version.0, &url])
            .await?;
        Ok(())
    }

    async fn get_compiled_binary_ref(
        &self,
        program_id: ProgramId,
        version: Version,
    ) -> Result<Option<String>, DBError> {
        let conn = self.pool.get().await?;
        let stmt = conn
            .prepare_cached(
                "SELECT url FROM compiled_binary WHERE program_id = $1 AND version = $2",
            )
            .await?;
        let res = conn.query_opt(&stmt, &[&program_id.0, &version.0]).await?;
        Ok(res.map(|e| e.get(0)))
    }

    async fn delete_compiled_binary_ref(
        &self,
        program_id: ProgramId,
        version: Version,
    ) -> Result<(), DBError> {
        let conn = self.pool.get().await?;
        let stmt = conn
            .prepare_cached("DELETE FROM compiled_binary WHERE program_id = $1 AND version = $2")
            .await?;
        let _res = conn.execute(&stmt, &[&program_id.0, &version.0]).await?;
        Ok(())
    }

    async fn check_connection(&self) -> Result<(), DBError> {
        let conn = self.pool.get().await?;
        let stmt = conn.prepare_cached("SELECT 1").await?;
        let _res = conn.execute(&stmt, &[]).await?;
        Ok(())
    }

    async fn new_service(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        name: &str,
        description: &str,
        config: &ServiceConfig,
    ) -> Result<ServiceId, DBError> {
        debug!("new_service {name} {description} {config:?}");
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("INSERT INTO service (id, name, description, config, tenant_id) VALUES($1, $2, $3, $4, $5)")
            .await?;
        manager
            .execute(
                &stmt,
                &[&id, &name, &description, &config.to_yaml(), &tenant_id.0],
            )
            .await
            .map_err(ProjectDB::maybe_unique_violation)
            .map_err(|e| {
                ProjectDB::maybe_tenant_id_foreign_key_constraint_err(e, tenant_id, None)
            })?;
        Ok(ServiceId(id))
    }

    async fn list_services(&self, tenant_id: TenantId) -> Result<Vec<ServiceDescr>, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT id, name, description, config FROM service WHERE tenant_id = $1",
            )
            .await?;
        let rows = manager.query(&stmt, &[&tenant_id.0]).await?;

        let mut result = Vec::with_capacity(rows.len());

        for row in rows {
            result.push(ServiceDescr {
                service_id: ServiceId(row.get(0)),
                name: row.get(1),
                description: row.get(2),
                config: ServiceConfig::from_yaml_str(row.get(3)),
            });
        }

        Ok(result)
    }

    async fn get_service_by_id(
        &self,
        tenant_id: TenantId,
        service_id: ServiceId,
    ) -> Result<ServiceDescr, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT name, description, config FROM service WHERE id = $1 AND tenant_id = $2",
            )
            .await?;

        let row = manager
            .query_opt(&stmt, &[&service_id.0, &tenant_id.0])
            .await?;

        if let Some(row) = row {
            Ok(ServiceDescr {
                service_id,
                name: row.get(0),
                description: row.get(1),
                config: ServiceConfig::from_yaml_str(row.get(2)),
            })
        } else {
            Err(DBError::UnknownService { service_id })
        }
    }

    async fn get_service_by_name(
        &self,
        tenant_id: TenantId,
        name: String,
    ) -> Result<ServiceDescr, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT id, description, config FROM service WHERE name = $1 AND tenant_id = $2",
            )
            .await?;
        let row = manager.query_opt(&stmt, &[&name, &tenant_id.0]).await?;

        if let Some(row) = row {
            Ok(ServiceDescr {
                service_id: ServiceId(row.get(0)),
                name,
                description: row.get(1),
                config: ServiceConfig::from_yaml_str(row.get(2)),
            })
        } else {
            Err(DBError::UnknownName { name })
        }
    }

    async fn update_service(
        &self,
        tenant_id: TenantId,
        service_id: ServiceId,
        description: &str,
        config: &Option<ServiceConfig>,
    ) -> Result<(), DBError> {
        let manager = self.pool.get().await?;
        let modified_rows = match config {
            None => {
                let stmt = manager
                    .prepare_cached(
                        "UPDATE service SET description = $1 WHERE id = $2 AND tenant_id = $3",
                    )
                    .await?;
                manager
                    .execute(&stmt, &[&description, &service_id.0, &tenant_id.0])
                    .await
                    .map_err(DBError::from)?
            }
            Some(config) => {
                let stmt = manager
                    .prepare_cached(
                        "UPDATE service SET description = $1, config = $2 WHERE id = $3 AND tenant_id = $4",
                    )
                    .await?;
                manager
                    .execute(
                        &stmt,
                        &[&description, &config.to_yaml(), &service_id.0, &tenant_id.0],
                    )
                    .await
                    .map_err(DBError::from)?
            }
        };
        if modified_rows > 0 {
            Ok(())
        } else {
            Err(DBError::UnknownService { service_id })
        }
    }

    async fn delete_service(
        &self,
        tenant_id: TenantId,
        service_id: ServiceId,
    ) -> Result<(), DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("DELETE FROM service WHERE id = $1 AND tenant_id = $2")
            .await?;
        let res = manager
            .execute(&stmt, &[&service_id.0, &tenant_id.0])
            .await?;

        if res > 0 {
            Ok(())
        } else {
            Err(DBError::UnknownService { service_id })
        }
    }
}

impl ProjectDB {
    pub async fn connect(
        db_config: &DatabaseConfig,
        #[cfg(feature = "pg-embed")] api_config: Option<&ApiServerConfig>,
    ) -> Result<Self, DBError> {
        let connection_str = db_config.database_connection_string();

        #[cfg(feature = "pg-embed")]
        if connection_str.starts_with("postgres-embed") {
            let database_dir = api_config
                .expect("ApiServerConfig needs to be provided when using pg-embed")
                .postgres_embed_data_dir();
            let pg_inst = pg_setup::install(database_dir, true, Some(8082)).await?;
            let connection_string = pg_inst.db_uri.to_string();
            return Self::connect_inner(connection_string.as_str(), Some(pg_inst)).await;
        };

        Self::connect_inner(
            connection_str.as_str(),
            #[cfg(feature = "pg-embed")]
            None,
        )
        .await
    }

    /// Connect to the project database.
    ///
    /// # Arguments
    /// - `config` a tokio postgres config
    ///
    /// # Notes
    /// Maybe this should become the preferred way to create a ProjectDb
    /// together with `pg-client-config` (and drop `connect_inner`).
    #[cfg(all(test, not(feature = "pg-embed")))]
    async fn with_config(config: tokio_postgres::Config) -> Result<Self, DBError> {
        let db = ProjectDB::initialize(
            config,
            #[cfg(feature = "pg-embed")]
            None,
        )
        .await?;
        Ok(db)
    }

    /// Connect to the project database.
    ///
    /// # Arguments
    /// - `connection_str`: The connection string to the database.
    async fn connect_inner(
        connection_str: &str,
        #[cfg(feature = "pg-embed")] pg_inst: Option<pg_embed::postgres::PgEmbed>,
    ) -> Result<Self, DBError> {
        if !connection_str.starts_with("postgres") {
            panic!("Unsupported connection string {}", connection_str)
        }
        let config = connection_str.parse::<tokio_postgres::Config>()?;
        debug!("Opening connection to {:?}", connection_str);

        let db = ProjectDB::initialize(
            config,
            #[cfg(feature = "pg-embed")]
            pg_inst,
        )
        .await?;
        Ok(db)
    }

    async fn initialize(
        config: tokio_postgres::Config,
        #[cfg(feature = "pg-embed")] pg_inst: Option<pg_embed::postgres::PgEmbed>,
    ) -> Result<Self, DBError> {
        let mgr_config = deadpool_postgres::ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let mgr = Manager::from_config(config.clone(), NoTls, mgr_config);
        let pool = Pool::builder(mgr).max_size(16).build().unwrap();
        #[cfg(feature = "pg-embed")]
        return Ok(Self {
            config,
            pool,
            pg_inst,
        });
        #[cfg(not(feature = "pg-embed"))]
        return Ok(Self { config, pool });
    }

    fn deserialize_error_response(
        pipeline_id: PipelineId,
        error_str: &str,
    ) -> Result<ErrorResponse, DBError> {
        serde_json::from_str::<ErrorResponse>(error_str).map_err(|_| {
            DBError::invalid_data(format!(
                "Unexpected pipeline error format for pipeline '{pipeline_id}': {error_str}"
            ))
        })
    }

    async fn row_to_pipeline_descr(&self, row: &Row) -> Result<PipelineDescr, DBError> {
        let pipeline_id = PipelineId(row.get(0));
        let program_name = row.get::<_, Option<String>>(5);
        Ok(PipelineDescr {
            pipeline_id,
            program_name,
            version: Version(row.get(1)),
            name: row.get(2),
            description: row.get(3),
            config: RuntimeConfig::from_yaml(row.get(4)),
            attached_connectors: self.json_to_attached_connectors(row.get(6)).await?,
        })
    }

    async fn row_to_pipeline_runtime_state(
        &self,
        pipeline_id: PipelineId,
        row: &Option<Row>,
    ) -> Result<PipelineRuntimeState, DBError> {
        if let Some(row) = row {
            Ok(PipelineRuntimeState {
                location: row.get::<_, Option<String>>(0).unwrap_or_default(),
                desired_status: row.get::<_, String>(1).try_into()?,
                current_status: row.get::<_, String>(2).try_into()?,
                status_since: convert_bigint_to_time(row.get(3))?,
                error: row
                    .get::<_, Option<String>>(4)
                    .map(|s| Self::deserialize_error_response(pipeline_id, &s))
                    .transpose()?,
                created: convert_bigint_to_time(row.get(5))?,
            })
        } else {
            Err(DBError::UnknownPipeline { pipeline_id })
        }
    }

    async fn row_to_pipeline(&self, row: &Row) -> Result<Pipeline, DBError> {
        let pipeline_id = PipelineId(row.get(0));
        let program_name = row.get::<_, Option<String>>(5);
        let descriptor = PipelineDescr {
            pipeline_id,
            program_name,
            version: Version(row.get(1)),
            name: row.get(2),
            description: row.get(3),
            config: RuntimeConfig::from_yaml(row.get(4)),
            attached_connectors: self.json_to_attached_connectors(row.get(6)).await?,
        };

        let state = PipelineRuntimeState {
            location: row.get::<_, Option<String>>(7).unwrap_or_default(),
            desired_status: row.get::<_, String>(8).try_into()?,
            current_status: row.get::<_, String>(9).try_into()?,
            status_since: convert_bigint_to_time(row.get(10))?,
            error: row
                .get::<_, Option<String>>(11)
                .map(|s| Self::deserialize_error_response(pipeline_id, &s))
                .transpose()?,
            created: convert_bigint_to_time(row.get(12))?,
        };

        Ok(Pipeline { descriptor, state })
    }

    /// We check if a program is 'in use' by checking if it is referenced by a
    /// pipeline or if it is used by the last committed revision of a pipeline.
    ///
    /// # Notes
    /// - `program_id` and `version` are raw types here (not `ProgramId`,
    /// `Version`) because it comes from files we currently have an invariant
    /// that only valid program ids are represented with the ProgramId type.
    pub(crate) async fn is_program_version_in_use(
        &self,
        program_id: Uuid,
        version: i64,
    ) -> Result<bool, DBError> {
        let conn = self.pool.get().await?;
        let stmt = conn
            .prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM program prog
                WHERE prog.id = $1
                AND prog.version = $2)
            OR
            EXISTS(SELECT 1 FROM pipeline p, pipeline_history ph, program_history progh
                WHERE ph.program_id = $1
                AND ph.revision = p.last_revision
                AND ph.program_id = progh.id
                AND progh.version = $2)",
            )
            .await?;

        let row = conn.query_one(&stmt, &[&program_id, &version]).await?;

        Ok(row.get(0))
    }

    pub(crate) async fn pipeline_is_committable(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<(PipelineDescr, ProgramDescr, Vec<ConnectorDescr>), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let pipeline = self
            .get_pipeline_descr_by_id(tenant_id, pipeline_id, Some(&txn))
            .await?;
        let program_name = pipeline
            .program_name
            .as_ref()
            .ok_or(DBError::ProgramNotSet)?;
        let program = self
            .get_program_by_name(tenant_id, program_name, true, Some(&txn))
            .await?;
        let connectors = self
            .get_connectors_for_pipeline_id(tenant_id, pipeline_id, Some(&txn))
            .await?;
        txn.commit().await?;
        // Check that this configuration forms a valid snapshot
        PipelineRevision::validate(&pipeline, &connectors, &program)?;
        Ok((pipeline, program, connectors))
    }

    pub(crate) async fn pipeline_config(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<PipelineConfig, DBError> {
        let pipeline = self
            .get_pipeline_descr_by_id(tenant_id, pipeline_id, None)
            .await?;
        let connectors: Vec<ConnectorDescr> = self
            .get_connectors_for_pipeline_id(tenant_id, pipeline_id, None)
            .await?;
        PipelineRevision::generate_pipeline_config(&pipeline, &connectors)
    }

    /// Retrieve all connectors referenced by a pipeline.
    async fn get_connectors_for_pipeline_id(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        txn: Option<&Transaction<'_>>,
    ) -> Result<Vec<ConnectorDescr>, DBError> {
        let query = "SELECT c.id, c.name, c.description, c.config
            FROM connector c, attached_connector ac
            WHERE ac.pipeline_id = $1
            AND ac.connector_id = c.id
            AND c.tenant_id = $2";
        let rows = if let Some(txn) = txn {
            let stmt = txn.prepare_cached(query).await?;
            txn.query(&stmt, &[&pipeline_id.0, &tenant_id.0]).await?
        } else {
            let manager = self.pool.get().await?;
            let stmt = manager.prepare_cached(query).await?;
            manager
                .query(&stmt, &[&pipeline_id.0, &tenant_id.0])
                .await?
        };

        Ok(rows
            .iter()
            .map(|row| {
                let connector_id = ConnectorId(row.get(0));
                let name = row.get(1);
                let description = row.get(2);
                let config = ConnectorConfig::from_yaml_str(row.get(3));

                ConnectorDescr {
                    connector_id,
                    name,
                    description,
                    config,
                }
            })
            .collect::<Vec<ConnectorDescr>>())
    }

    /// Attach connector to the pipeline.
    ///
    /// # Precondition
    /// - A valid pipeline for `pipeline_id` must exist.
    async fn attach_connector(
        &self,
        tenant_id: TenantId,
        txn: &Transaction<'_>,
        pipeline_id: PipelineId,
        ac: &AttachedConnector,
    ) -> Result<(), DBError> {
        let stmt = txn.prepare_cached("INSERT INTO attached_connector (name, pipeline_id, connector_id, is_input, config, tenant_id)
            SELECT $2, $3, id, $5, $6, tenant_id
            FROM connector
            WHERE tenant_id = $1 AND name = $4")
        .await?;
        let rows = txn
            .execute(
                &stmt,
                &[
                    &tenant_id.0,
                    &ac.name,
                    &pipeline_id.0,
                    &ac.connector_name,
                    &ac.is_input,
                    &ac.relation_name,
                ],
            )
            .map_err(Self::maybe_unique_violation)
            .map_err(|e| Self::maybe_pipeline_id_foreign_key_constraint_err(e, pipeline_id))
            .await?;
        if rows == 0 {
            Err(DBError::UnknownConnectorName {
                connector_name: ac.connector_name.to_string(),
            })
        } else {
            Ok(())
        }
    }

    async fn json_to_attached_connectors(
        &self,
        connectors_json: Value,
    ) -> Result<Vec<AttachedConnector>, DBError> {
        let connector_arr = connectors_json.as_array().unwrap();
        let mut attached_connectors = Vec::with_capacity(connector_arr.len());
        for connector in connector_arr {
            let obj = connector.as_object().unwrap();
            let is_input: bool = obj.get("is_input").unwrap().as_bool().unwrap();

            // let uuid_str = obj.get("connector_id").unwrap().as_str().unwrap();
            // let connector_id = ConnectorId(Uuid::parse_str(uuid_str).map_err(|e| {
            //     DBError::invalid_data(format!("error parsing connector id '{uuid_str}':
            // {e}")) })?);
            let connector_name = obj
                .get("connector_name")
                .unwrap()
                .as_str()
                .unwrap()
                .to_string();

            attached_connectors.push(AttachedConnector {
                name: obj.get("name").unwrap().as_str().unwrap().to_owned(),
                connector_name,
                relation_name: obj.get("config").unwrap().as_str().unwrap().to_owned(),
                is_input,
            });
        }
        Ok(attached_connectors)
    }

    /// Helper to convert postgres error into a `DBError` if the underlying
    /// low-level error thrown by the database matches.
    fn maybe_unique_violation(err: PgError) -> DBError {
        if let Some(dberr) = err.as_db_error() {
            if dberr.code() == &tokio_postgres::error::SqlState::UNIQUE_VIOLATION {
                match dberr.constraint() {
                    Some("program_pkey") => DBError::unique_key_violation("program_pkey"),
                    Some("connector_pkey") => DBError::unique_key_violation("connector_pkey"),
                    Some("service_pkey") => DBError::unique_key_violation("service_pkey"),
                    Some("pipeline_pkey") => DBError::unique_key_violation("pipeline_pkey"),
                    Some("api_key_pkey") => DBError::unique_key_violation("api_key_pkey"),
                    Some("unique_hash") => DBError::duplicate_key(),
                    Some(_constraint) => DBError::DuplicateName,
                    None => DBError::DuplicateName,
                }
            } else {
                DBError::from(err)
            }
        } else {
            DBError::from(err)
        }
    }

    /// Helper to convert program_id foreign key constraint error into an
    /// user-friendly error message.
    fn maybe_program_id_not_found_foreign_key_constraint_err(
        err: DBError,
        program_id: Option<ProgramId>,
    ) -> DBError {
        if let DBError::PostgresError { error, .. } = &err {
            let db_err = error.as_db_error();
            if let Some(db_err) = db_err {
                if db_err.code() == &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION
                    && db_err.constraint() == Some("pipeline_program_id_tenant_id_fkey")
                {
                    if let Some(program_id) = program_id {
                        return DBError::UnknownProgram { program_id };
                    } else {
                        unreachable!("program_id cannot be none");
                    }
                }
            }
        }

        err
    }

    /// Helper to convert program_id foreign key constraint error into an
    /// user-friendly error message.
    fn maybe_program_id_in_use_foreign_key_constraint_err(
        err: DBError,
        program_id: Option<ProgramId>,
    ) -> DBError {
        if let DBError::PostgresError { error, .. } = &err {
            let db_err = error.as_db_error();
            if let Some(db_err) = db_err {
                if db_err.code() == &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION
                    && db_err.constraint() == Some("pipeline_program_id_tenant_id_fkey")
                {
                    if let Some(program_id) = program_id {
                        return DBError::ProgramInUseByPipeline { program_id };
                    } else {
                        unreachable!("program_id cannot be none");
                    }
                }
            }
        }

        err
    }

    /// Helper to convert pipeline_id foreign key constraint error into an
    /// user-friendly error message.
    fn maybe_pipeline_id_foreign_key_constraint_err(
        err: DBError,
        pipeline_id: PipelineId,
    ) -> DBError {
        if let DBError::PostgresError { error, .. } = &err {
            let db_err = error.as_db_error();
            if let Some(db_err) = db_err {
                if db_err.code() == &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION
                    && (db_err.constraint() == Some("pipeline_pipeline_id_fkey")
                        || db_err.constraint() == Some("attached_connector_pipeline_id_fkey"))
                {
                    return DBError::UnknownPipeline { pipeline_id };
                }
            }
        }

        err
    }

    /// Helper to convert tenant_id foreign key constraint error into an
    /// user-friendly error message.
    fn maybe_tenant_id_foreign_key_constraint_err(
        err: DBError,
        tenant_id: TenantId,
        missing_id: Option<Uuid>,
    ) -> DBError {
        if let DBError::PostgresError { error, .. } = &err {
            let db_err = error.as_db_error();
            if let Some(db_err) = db_err {
                if db_err.code() == &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION {
                    if let Some(constraint_name) = db_err.constraint() {
                        if constraint_name.ends_with("pipeline_program_id_tenant_id_fkey") {
                            return DBError::UnknownProgram {
                                program_id: ProgramId(missing_id.unwrap()),
                            };
                        }
                        if constraint_name.ends_with("tenant_id_fkey") {
                            return DBError::UnknownTenant { tenant_id };
                        }
                    }
                }
            }
        }

        err
    }

    /// Run database migrations
    pub async fn run_migrations(&self) -> Result<(), DBError> {
        info!("Running DB migrations");
        let mut client = self.pool.get().await?;
        embedded::migrations::runner()
            .run_async(&mut **client)
            .await?;
        let default_tenant = TenantRecord::default();
        self.create_tenant_if_not_exists(
            default_tenant.id.0,
            default_tenant.tenant,
            default_tenant.provider,
        )
        .await?;
        Ok(())
    }

    /// Check if the expected DB migrations have already been run
    pub async fn check_migrations(self) -> Result<Self, DBError> {
        debug!("Checking if DB migrations have been applied");
        let mut client = self.pool.get().await?;
        let runner = embedded::migrations::runner();
        let expected_max_version = runner
            .get_migrations()
            .iter()
            .map(|m| m.version())
            .fold(std::u32::MIN, |a, b| a.max(b));
        let migration = runner.get_last_applied_migration_async(&mut **client).await;
        if let Ok(Some(m)) = migration {
            let v = m.version();
            info!("Expected version = {expected_max_version}. Actual version = {v}.");
            if v == expected_max_version {
                Ok(self)
            } else {
                Err(DBError::MissingMigrations {
                    expected: expected_max_version,
                    actual: v,
                })
            }
        } else {
            info!("Expected version = {expected_max_version}. Actual version = None.");
            Err(DBError::MissingMigrations {
                expected: expected_max_version,
                actual: 0,
            })
        }
    }
}
