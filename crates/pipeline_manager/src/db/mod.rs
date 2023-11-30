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
use log::{debug, error, info};
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
};
use storage::Storage;
use tokio_postgres::{error::Error as PgError, NoTls, Row};
use utoipa::ToSchema;
use uuid::Uuid;

#[cfg(test)]
use proptest::collection::vec;
#[cfg(test)]
use proptest::prelude::any;
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

/// Unique program id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct ProgramId(#[cfg_attr(test, proptest(strategy = "test::limited_uuid()"))] pub Uuid);
impl Display for ProgramId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
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

/// The database encodes program status using two columns: `status`, which has
/// type `string`, but acts as an enum, and `error`, only used if `status` is
/// one of `"sql_error"` or `"rust_error"`.
impl ProgramStatus {
    /// Decode `ProgramStatus` from the values of `error` and `status` columns.
    fn from_columns(
        status_string: Option<&str>,
        error_string: Option<String>,
    ) -> Result<Self, DBError> {
        match status_string {
            None => Ok(Self::None),
            Some("success") => Ok(Self::Success),
            Some("pending") => Ok(Self::Pending),
            Some("compiling_sql") => Ok(Self::CompilingSql),
            Some("compiling_rust") => Ok(Self::CompilingRust),
            Some("sql_error") => {
                let error = error_string.unwrap_or_default();
                if let Ok(messages) = serde_json::from_str(&error) {
                    Ok(Self::SqlError(messages))
                } else {
                    error!("Expected valid json for SqlCompilerMessage but got {:?}, did you update the struct without adjusting the database?", error);
                    Ok(Self::SystemError(error))
                }
            }
            Some("rust_error") => Ok(Self::RustError(error_string.unwrap_or_default())),
            Some("system_error") => Ok(Self::SystemError(error_string.unwrap_or_default())),
            Some(status) => Err(DBError::invalid_status(status.to_string())),
        }
    }
    fn to_columns(&self) -> (Option<String>, Option<String>) {
        match self {
            ProgramStatus::None => (None, None),
            ProgramStatus::Success => (Some("success".to_string()), None),
            ProgramStatus::Pending => (Some("pending".to_string()), None),
            ProgramStatus::CompilingSql => (Some("compiling_sql".to_string()), None),
            ProgramStatus::CompilingRust => (Some("compiling_rust".to_string()), None),
            ProgramStatus::SqlError(error) => {
                if let Ok(error_string) = serde_json::to_string(&error) {
                    (Some("sql_error".to_string()), Some(error_string))
                } else {
                    error!("Expected valid json for SqlError, but got {:?}", error);
                    (Some("sql_error".to_string()), None)
                }
            }
            ProgramStatus::RustError(error) => {
                (Some("rust_error".to_string()), Some(error.clone()))
            }
            ProgramStatus::SystemError(error) => {
                (Some("system_error".to_string()), Some(error.clone()))
            }
        }
    }
}

/// A struct containting the tables (inputs) and views for a program.
///
/// Parse from the JSON data-type of the DDL generated by the SQL compiler.
#[derive(Serialize, Deserialize, ToSchema, Debug, Eq, PartialEq, Clone)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) struct ProgramSchema {
    #[cfg_attr(test, proptest(strategy = "vec(any::<Relation>(), 0..2)"))]
    pub inputs: Vec<Relation>,
    #[cfg_attr(test, proptest(strategy = "vec(any::<Relation>(), 0..2)"))]
    pub outputs: Vec<Relation>,
}

/// A SQL table or view. It has a name and a list of fields.
///
/// Matches the Calcite JSON format.
#[derive(Serialize, Deserialize, ToSchema, Debug, Eq, PartialEq, Clone)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) struct Relation {
    #[cfg_attr(test, proptest(regex = "relation1|relation2|relation3"))]
    pub name: String,
    #[cfg_attr(test, proptest(value = "Vec::new()"))]
    pub fields: Vec<Field>,
}
/// A SQL field.
///
/// Matches the Calcite JSON format.
#[derive(Serialize, Deserialize, ToSchema, Debug, Eq, PartialEq, Clone)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) struct Field {
    pub name: String,
    pub columntype: ColumnType,
}

/// A SQL column type description.
///
/// Matches the Calcite JSON format.
#[derive(Serialize, Deserialize, ToSchema, Debug, Eq, PartialEq, Clone)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) struct ColumnType {
    #[serde(rename = "type")]
    /// Identifier for the type (e.g., `VARCHAR`, `BIGINT`, `ARRAY` etc.)
    pub typ: String,
    /// Does the type accept NULL values?
    pub nullable: bool,
    /// Precision of the type.
    ///
    /// # Examples
    /// - `VARCHAR` sets precision to `-1`.
    /// - `VARCHAR(255)` sets precision to `255`.
    /// - `BIGINT`, `DATE`, `FLOAT`, `DOUBLE`, `GEOMETRY`, etc. sets precision
    ///   to None
    /// - `TIME`, `TIMESTAMP` set precision to `0`.
    pub precision: Option<i64>,
    /// The scale of the type.
    ///
    /// # Example
    /// - `DECIMAL(1,2)` sets scale to `2`.
    pub scale: Option<i64>,
    /// A component of the type (if available).
    ///
    /// This is in a `Box` because it makes it a recursive types.
    ///
    /// For example, this would specify the `VARCHAR(20)` in the `VARCHAR(20)
    /// ARRAY` type.
    #[cfg_attr(test, proptest(value = "None"))]
    pub component: Option<Box<ColumnType>>,
}

/// Program descriptor.
#[derive(Deserialize, Serialize, ToSchema, Debug, Eq, PartialEq, Clone)]
pub(crate) struct ProgramDescr {
    /// Unique program id.
    pub program_id: ProgramId,
    /// Program name (doesn't have to be unique).
    pub name: String,
    /// Program description.
    pub description: String,
    /// Program version, incremented every time program code is modified.
    pub version: Version,
    /// Program compilation status.
    pub status: ProgramStatus,
    /// A JSON description of the SQL tables and view declarations including
    /// field names and types.
    ///
    /// The schema is set/updated whenever the `status` field reaches >=
    /// `ProgramStatus::CompilingRust`.
    ///
    /// # Example
    ///
    /// The given SQL program:
    ///
    /// ```ignore
    /// CREATE TABLE USERS ( name varchar );
    /// CREATE VIEW OUTPUT_USERS as SELECT * FROM USERS;
    /// ```
    ///
    /// Would lead the following JSON string in `schema`:
    ///
    /// ```ignore
    /// {
    ///   "inputs": [{
    ///       "name": "USERS",
    ///       "fields": [{ "name": "NAME", "type": "VARCHAR", "nullable": true }]
    ///     }],
    ///   "outputs": [{
    ///       "name": "OUTPUT_USERS",
    ///       "fields": [{ "name": "NAME", "type": "VARCHAR", "nullable": true }]
    ///     }]
    /// }
    /// ```
    pub schema: Option<ProgramSchema>,

    /// SQL code
    pub code: Option<String>,
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
        if pipeline.program_id.is_none() {
            return Err(DBError::ProgramNotSet);
        }
        // ..  and matches the provided program id (this is an assert because
        // it's an error that can't be caused by a end-user)
        assert_eq!(
            pipeline.program_id.unwrap(),
            program.program_id,
            "pre-condition: pipeline program and program_descr match"
        );
        // We supplied all connectors referenced by the `pipeline`, also an assert
        // (not possible to trigger by end-user)
        assert_eq!(
            HashSet::<Uuid>::from_iter(
                pipeline
                    .attached_connectors
                    .iter()
                    .map(|ac| ac.connector_id.0)
            ),
            HashSet::from_iter(connectors.iter().map(|c| c.connector_id.0)),
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
            let connector = connectors
                .iter()
                .find(|c| ac.connector_id == c.connector_id);
            if connector.is_none() {
                return Err(DBError::UnknownConnector {
                    connector_id: ac.connector_id,
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
            let connector = connectors
                .iter()
                .find(|c| ac.connector_id == c.connector_id);
            if connector.is_none() {
                return Err(DBError::UnknownConnector {
                    connector_id: ac.connector_id,
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
    pub program_id: Option<ProgramId>,
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
    /// Is this an input or an output?
    pub is_input: bool,
    /// The id of the connector to attach.
    pub connector_id: ConnectorId,
    /// The table or view this connector is attached to.
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

/// Permission types for invoking pipeline manager APIs
#[derive(Serialize, ToSchema, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) enum ApiPermission {
    Read,
    Write,
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

// The goal for these methods is to avoid multiple DB interactions as much as
// possible and if not, use transactions
#[async_trait]
impl Storage for ProjectDB {
    async fn list_programs(
        &self,
        tenant_id: TenantId,
        with_code: bool,
    ) -> Result<Vec<ProgramDescr>, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                r#"SELECT id, name, description, version, status, error, schema,
                CASE WHEN $2 IS TRUE THEN code ELSE null END
                FROM program WHERE tenant_id = $1"#,
            )
            .await?;
        let rows = manager.query(&stmt, &[&tenant_id.0, &with_code]).await?;

        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let status: Option<String> = row.get(4);
            let error: Option<String> = row.get(5);
            let status = ProgramStatus::from_columns(status.as_deref(), error)?;
            let schema: Option<ProgramSchema> = row
                .get::<_, Option<String>>(6)
                .map(|s| serde_json::from_str(&s))
                .transpose()
                .map_err(|e| DBError::invalid_data(format!("Error parsing program schema: {e}")))?;

            result.push(ProgramDescr {
                program_id: ProgramId(row.get(0)),
                name: row.get(1),
                description: row.get(2),
                version: Version(row.get(3)),
                schema,
                status,
                code: row.get(7),
            });
        }

        Ok(result)
    }

    async fn new_program(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        program_name: &str,
        program_description: &str,
        program_code: &str,
    ) -> Result<(ProgramId, Version), DBError> {
        debug!("new_program {program_name} {program_description} {program_code}");
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "INSERT INTO program (id, version, tenant_id, name, description, code, schema, status, error, status_since)
                        VALUES($1, 1, $2, $3, $4, $5, NULL, NULL, NULL, now());",
            )
            .await?;
        manager
            .execute(
                &stmt,
                &[
                    &id,
                    &tenant_id.0,
                    &program_name,
                    &program_description,
                    &program_code,
                ],
            )
            .await
            .map_err(ProjectDB::maybe_unique_violation)
            .map_err(|e| {
                ProjectDB::maybe_tenant_id_foreign_key_constraint_err(e, tenant_id, None)
            })?;

        Ok((ProgramId(id), Version(1)))
    }

    /// Update program name, description and, optionally, code.
    /// XXX: Description should be optional too
    async fn update_program(
        &self,
        tenant_id: TenantId,
        program_id: ProgramId,
        program_name: &str,
        program_description: &str,
        program_code: &Option<String>,
    ) -> Result<Version, DBError> {
        let row = match program_code {
            Some(code) => {
                let manager = self.pool.get().await?;
                // Only increment `version` if new code actually differs from the
                // current version.
                let stmt = manager
                    .prepare_cached(
                        "UPDATE program
                        SET
                            version = (CASE WHEN code = $3 THEN version ELSE version + 1 END),
                            name = $1,
                            description = $2,
                            code = $3,
                            status = (CASE WHEN code = $3 THEN status ELSE NULL END),
                            error = (CASE WHEN code = $3 THEN error ELSE NULL END),
                            schema = (CASE WHEN code = $3 THEN schema ELSE NULL END)
                    WHERE id = $4 AND tenant_id = $5
                    RETURNING version
                ",
                    )
                    .await?;

                manager
                    .query_opt(
                        &stmt,
                        &[
                            &program_name,
                            &program_description,
                            &code,
                            &program_id.0,
                            &tenant_id.0,
                        ],
                    )
                    .await
                    .map_err(ProjectDB::maybe_unique_violation)?
            }
            _ => {
                let manager = self.pool.get().await?;
                let stmt = manager
                    .prepare_cached(
                        "UPDATE program SET name = $1, description = $2 WHERE id = $3 AND tenant_id = $4 RETURNING version",
                    )
                    .await?;
                manager
                    .query_opt(
                        &stmt,
                        &[
                            &program_name,
                            &program_description,
                            &program_id.0,
                            &tenant_id.0,
                        ],
                    )
                    .await
                    .map_err(ProjectDB::maybe_unique_violation)?
            }
        };

        if let Some(row) = row {
            Ok(Version(row.get(0)))
        } else {
            Err(DBError::UnknownProgram { program_id })
        }
    }

    /// Retrieve program descriptor.
    ///
    /// Returns `None` if `program_id` is not found in the database.
    async fn get_program_if_exists(
        &self,
        tenant_id: TenantId,
        program_id: ProgramId,
        with_code: bool,
    ) -> Result<Option<ProgramDescr>, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT name, description, version, status, error, schema,
                CASE WHEN $3 IS TRUE THEN code ELSE null END
                FROM program WHERE id = $1 AND tenant_id = $2",
            )
            .await?;
        let row = manager
            .query_opt(&stmt, &[&program_id.0, &tenant_id.0, &with_code])
            .await?;

        if let Some(row) = row {
            let name: String = row.get(0);
            let description: String = row.get(1);
            let version: Version = Version(row.get(2));
            let status: Option<String> = row.get(3);
            let error: Option<String> = row.get(4);
            let schema: Option<ProgramSchema> = row
                .get::<_, Option<String>>(5)
                .map(|s| serde_json::from_str(&s))
                .transpose()
                .map_err(|e| DBError::invalid_data(format!("Error parsing program schema: {e}")))?;
            let code: Option<String> = row.get(6);

            let status = ProgramStatus::from_columns(status.as_deref(), error)?;
            Ok(Some(ProgramDescr {
                program_id,
                name,
                description,
                version,
                status,
                schema,
                code,
            }))
        } else {
            Ok(None)
        }
    }

    /// Lookup program by name.
    async fn lookup_program(
        &self,
        tenant_id: TenantId,
        program_name: &str,
        with_code: bool,
    ) -> Result<Option<ProgramDescr>, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT id, description, version, status, error, schema, tenant_id,
                 CASE WHEN $3 IS TRUE THEN code ELSE null END
                 FROM program WHERE name = $1 AND tenant_id = $2",
            )
            .await?;
        let row = manager
            .query_opt(&stmt, &[&program_name, &tenant_id.0, &with_code])
            .await?;

        if let Some(row) = row {
            let program_id: ProgramId = ProgramId(row.get(0));
            let description: String = row.get(1);
            let version: Version = Version(row.get(2));
            let status: Option<String> = row.get(3);
            let error: Option<String> = row.get(4);
            let schema: Option<ProgramSchema> = row
                .get::<_, Option<String>>(5)
                .map(|s| serde_json::from_str(&s))
                .transpose()
                .map_err(|e| DBError::invalid_data(format!("Error parsing program schema: {e}")))?;
            let code: Option<String> = row.get(7);

            let status = ProgramStatus::from_columns(status.as_deref(), error)?;
            Ok(Some(ProgramDescr {
                program_id,
                name: program_name.to_string(),
                description,
                version,
                status,
                schema,
                code,
            }))
        } else {
            Ok(None)
        }
    }

    async fn set_program_for_compilation(
        &self,
        tenant_id: TenantId,
        program_id: ProgramId,
        expected_version: Version,
        status: ProgramStatus,
    ) -> Result<(), DBError> {
        let (status, error) = status.to_columns();
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "UPDATE program SET
                 status = (CASE WHEN version = $4 THEN $1 ELSE status END),
                 error = (CASE WHEN version = $4 THEN $2 ELSE error END),
                 status_since = (CASE WHEN version = $4 THEN now()
                                 ELSE status_since END),
                 schema = (CASE WHEN version = $4 THEN NULL ELSE schema END)
                 WHERE id = $3 AND tenant_id = $5",
            )
            .await?;

        manager
            .execute(
                &stmt,
                &[
                    &status,
                    &error,
                    &program_id.0,
                    &expected_version.0,
                    &tenant_id.0,
                ],
            )
            .await?;

        Ok(())
    }

    async fn set_program_status_guarded(
        &self,
        tenant_id: TenantId,
        program_id: ProgramId,
        expected_version: Version,
        status: ProgramStatus,
    ) -> Result<(), DBError> {
        let (status, error) = status.to_columns();
        let manager = self.pool.get().await?;
        // We could perform the guard in the WHERE clause, but that does not
        // tell us whether the ID existed or not.
        // Instead, we use a case statement for the guard.
        let stmt = manager
            .prepare_cached(
                "UPDATE program SET
                 status = (CASE WHEN version = $4 THEN $1 ELSE status END),
                 error = (CASE WHEN version = $4 THEN $2 ELSE error END),
                 status_since = (CASE WHEN version = $4 THEN now()
                                 ELSE status_since END)
                 WHERE id = $3 AND tenant_id = $5 RETURNING id",
            )
            .await?;

        let row = manager
            .query_opt(
                &stmt,
                &[
                    &status,
                    &error,
                    &program_id.0,
                    &expected_version.0,
                    &tenant_id.0,
                ],
            )
            .await?;
        if row.is_none() {
            Err(DBError::UnknownProgram { program_id })
        } else {
            Ok(())
        }
    }

    async fn set_program_schema(
        &self,
        tenant_id: TenantId,
        program_id: ProgramId,
        schema: ProgramSchema,
    ) -> Result<(), DBError> {
        let schema = serde_json::to_string(&schema).map_err(|e| {
            DBError::invalid_data(format!(
                "Error serializing program schema '{schema:?}'.\nError: {e}"
            ))
        })?;
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("UPDATE program SET schema = $1 WHERE id = $2 AND tenant_id = $3")
            .await?;
        manager
            .execute(&stmt, &[&schema, &program_id.0, &tenant_id.0])
            .await?;

        Ok(())
    }

    async fn delete_program(
        &self,
        tenant_id: TenantId,
        program_id: ProgramId,
    ) -> Result<(), DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("DELETE FROM program WHERE id = $1 AND tenant_id = $2")
            .await?;
        let res = manager
            .execute(&stmt, &[&program_id.0, &tenant_id.0])
            .await
            .map_err(|e| {
                ProjectDB::maybe_program_id_in_use_foreign_key_constraint_err(
                    e.into(),
                    Some(program_id),
                )
            })?;
        if res > 0 {
            Ok(())
        } else {
            Err(DBError::UnknownProgram { program_id })
        }
    }

    async fn all_programs(&self) -> Result<Vec<(TenantId, ProgramDescr)>, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                r#"SELECT id, name, description, version, status, error, schema, tenant_id
                   FROM program"#,
            )
            .await?;
        let rows = manager.query(&stmt, &[]).await?;

        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let status: Option<String> = row.get(4);
            let error: Option<String> = row.get(5);
            let status = ProgramStatus::from_columns(status.as_deref(), error)?;
            let schema: Option<ProgramSchema> = row
                .get::<_, Option<String>>(6)
                .map(|s| serde_json::from_str(&s))
                .transpose()
                .map_err(|e| DBError::invalid_data(format!("Error parsing program schema: {e}")))?;
            let tenant_id = TenantId(row.get(7));

            result.push((
                tenant_id,
                ProgramDescr {
                    program_id: ProgramId(row.get(0)),
                    name: row.get(1),
                    description: row.get(2),
                    version: Version(row.get(3)),
                    schema,
                    status,
                    code: None,
                },
            ));
        }
        Ok(result)
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

    /// Version the current pipeline object and all state reachable from it.
    ///
    /// We store the last revision number in the pipeline object itself.
    async fn create_pipeline_revision(
        &self,
        revision: Uuid,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<Revision, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;

        // Finds the revision number
        let find_revision_number = txn
            .prepare_cached(
                "SELECT last_revision FROM pipeline WHERE id = $1 AND tenant_id = $2 FOR UPDATE",
            )
            .await?;
        // Takes a diff between the pipeline definition and its corresponding revision
        let compute_revision_current_diff = txn
            .prepare_cached(
                "WITH ph_entry AS (
                    SELECT progh.code, ch.config, ach.name, ach.config, ach.is_input, ph.config
                                        FROM pipeline_history ph
                                        INNER JOIN program_history progh ON ph.program_id = progh.id AND progh.revision = $2
                                        LEFT OUTER JOIN attached_connector_history ach ON ach.pipeline_id = ph.id AND ach.revision = $2
                                        LEFT OUTER JOIN connector_history ch ON ach.connector_id = ch.id AND ch.revision = $2
                                        WHERE ph.id = $1 AND ph.revision = $2
                ),
                p_entry AS (
                    SELECT prog.code, c.config, ac.name, ac.config, ac.is_input, p.config
                                        FROM pipeline p
                                        INNER JOIN program prog ON p.program_id = prog.id
                                        LEFT OUTER JOIN attached_connector ac ON ac.pipeline_id = p.id
                                        LEFT OUTER JOIN connector c ON ac.connector_id = c.id
                                        WHERE p.id = $1
                ),
                diff_1 AS (
                    SELECT * FROM ph_entry EXCEPT SELECT * FROM p_entry
                ),
                diff_2 AS (
                    SELECT * FROM p_entry EXCEPT SELECT * FROM ph_entry
                )
                SELECT COUNT(*) FROM (SELECT * FROM diff_1 UNION ALL SELECT * FROM diff_2) as x"
            )
            .await?;
        let insert_program_history = txn
            .prepare_cached(
                "INSERT INTO program_history SELECT $1 as revision, * FROM program p WHERE id = $2",
            )
            .await?;
        let insert_pipeline_history = txn
            .prepare_cached(
                "INSERT INTO pipeline_history SELECT $1 as revision, * FROM pipeline p WHERE id = $2",
            )
            .await?;
        let insert_connector_history = txn
            .prepare_cached(
                "INSERT INTO connector_history SELECT $1 as revision, c.* FROM connector c, attached_connector ac WHERE ac.pipeline_id = $2 AND ac.connector_id = c.id",
            )
            .await?;
        let insert_attached_connector_history = txn
            .prepare_cached(
                "INSERT INTO attached_connector_history SELECT $1 as revision, * FROM attached_connector ac WHERE ac.pipeline_id = $2",
            )
            .await?;
        let update_revision = txn
            .prepare_cached(
                "UPDATE pipeline SET last_revision = $1 WHERE id = $2 AND tenant_id = $3",
            )
            .await?;

        let revision_data = txn
            .query_opt(&find_revision_number, &[&pipeline_id.0, &tenant_id.0])
            .await?;
        let prev_revision = revision_data.and_then(|row: Row| row.get::<_, Option<Uuid>>(0));

        // Check if we actually changed something before writing a new revision
        //
        // Note: What fields are checked is ultimately determined by whatever is
        // used by the pipeline configuration, e.g., what the `start` function
        // uses in `runner.rs` to write the config/metadata:
        if prev_revision.is_some() {
            let change = txn
                .query_one(
                    &compute_revision_current_diff,
                    &[&pipeline_id.0, &prev_revision],
                )
                .await?;

            let nothing_changed = change.get::<_, i64>(0) == 0;
            if nothing_changed {
                return Err(DBError::RevisionNotChanged);
            }
        }

        // TODO: Ideally these queries happen in the same transaction `txn`.
        // Need to revise the API a bit to optionally pass in a transaction
        // object for this to work seamlessly. Also probably the program row
        // should be locked with FOR UPDATE while reading to prevent the
        // compiler from resetting the status field.
        let (_pipeline, program, _connectors) =
            self.pipeline_is_committable(tenant_id, pipeline_id).await?;

        // Copy all pipeline data to history tables
        //
        // TODO(performance): In theory the following inserts could all run in
        // parallel with async but I couldn't figure out how to make it work
        // with the args :/
        txn.execute(&insert_program_history, &[&revision, &program.program_id.0])
            .await?;
        txn.execute(&insert_pipeline_history, &[&revision, &pipeline_id.0])
            .await?;
        txn.execute(&insert_connector_history, &[&revision, &pipeline_id.0])
            .await?;
        txn.execute(
            &insert_attached_connector_history,
            &[&revision, &pipeline_id.0],
        )
        .await?;

        // Update the revision of the pipeline object
        txn.execute(&update_revision, &[&revision, &pipeline_id.0, &tenant_id.0])
            .await?;

        txn.commit().await?;

        Ok(Revision(revision))
    }

    async fn get_last_committed_pipeline_revision(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<PipelineRevision, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("SELECT last_revision FROM pipeline WHERE id = $1 AND tenant_id = $2")
            .await?;
        let row: Option<Row> = manager
            .query_opt(&stmt, &[&pipeline_id.0, &tenant_id.0])
            .await?;

        if let Some(row) = row {
            let revision = Revision(
                row.get::<_, Option<Uuid>>(0)
                    .ok_or(DBError::NoRevisionAvailable { pipeline_id })?,
            );
            let pipeline = self
                .get_committed_pipeline_by_id(tenant_id, pipeline_id, revision)
                .await?;
            // expect() is ok here - we don't allow to commit something without a program
            let program_id = pipeline
                .program_id
                .expect("pre-condition: pipeline has a program");
            let program = self
                .get_committed_program_by_id(tenant_id, program_id, revision)
                .await?;
            let connectors = self
                .get_committed_connectors_by_id(tenant_id, pipeline_id, revision)
                .await?;

            Ok(PipelineRevision::new(
                revision, pipeline, connectors, program,
            ))
        } else {
            Err(DBError::UnknownPipeline { pipeline_id })
        }
    }

    async fn list_pipelines(&self, tenant_id: TenantId) -> Result<Vec<Pipeline>, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT p.id, version, p.name, description, p.config, program_id,
            COALESCE(json_agg(json_build_object('name', ac.name,
                                                'connector_id', connector_id,
                                                'config', ac.config,
                                                'is_input', is_input))
                            FILTER (WHERE ac.name IS NOT NULL),
                    '[]'),
            rt.location, rt.desired_status, rt.current_status, rt.status_since, rt.error, rt.created
            FROM pipeline p
            INNER JOIN pipeline_runtime_state rt on p.id = rt.id
            LEFT JOIN attached_connector ac on p.id = ac.pipeline_id
            WHERE p.tenant_id = $1
            GROUP BY p.id, rt.id;",
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
                "SELECT p.id, version, p.name as cname, description, p.config, program_id,
                COALESCE(json_agg(json_build_object('name', ac.name,
                                                    'connector_id', connector_id,
                                                    'config', ac.config,
                                                    'is_input', is_input))
                                FILTER (WHERE ac.name IS NOT NULL),
                        '[]'),
                rt.location, rt.desired_status, rt.current_status, rt.status_since, rt.error, rt.created
                FROM pipeline p
                INNER JOIN pipeline_runtime_state rt on p.id = rt.id
                LEFT JOIN attached_connector ac on p.id = ac.pipeline_id
                WHERE p.id = $1 AND p.tenant_id = $2
                GROUP BY p.id, rt.id
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
    ) -> Result<PipelineDescr, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT p.id, version, p.name as cname, description, p.config, program_id,
                COALESCE(json_agg(json_build_object('name', ac.name,
                                                    'connector_id', connector_id,
                                                    'config', ac.config,
                                                    'is_input', is_input))
                                FILTER (WHERE ac.name IS NOT NULL),
                        '[]')
                FROM pipeline p
                LEFT JOIN attached_connector ac on p.id = ac.pipeline_id
                WHERE p.id = $1 AND p.tenant_id = $2
                GROUP BY p.id
                ",
            )
            .await?;

        let row = manager
            .query_opt(&stmt, &[&pipeline_id.0, &tenant_id.0])
            .await?
            .ok_or(DBError::UnknownPipeline { pipeline_id })?;

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
                "SELECT p.id, version, p.name as cname, description, p.config, program_id,
                COALESCE(json_agg(json_build_object('name', ac.name,
                                                    'connector_id', connector_id,
                                                    'config', ac.config,
                                                    'is_input', is_input))
                                FILTER (WHERE ac.name IS NOT NULL),
                        '[]')
                FROM pipeline p
                LEFT JOIN attached_connector ac on p.id = ac.pipeline_id
                WHERE p.name = $1 AND p.tenant_id = $2
                GROUP BY p.id
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
                "SELECT p.id, version, p.name as cname, description, p.config, program_id,
                COALESCE(json_agg(json_build_object('name', ac.name,
                                                    'connector_id', connector_id,
                                                    'config', ac.config,
                                                    'is_input', is_input))
                                FILTER (WHERE ac.name IS NOT NULL),
                        '[]'),
                rt.location, rt.desired_status, rt.current_status, rt.status_since, rt.error, rt.created
                FROM pipeline p
                INNER JOIN pipeline_runtime_state rt on p.id = rt.id
                LEFT JOIN attached_connector ac on p.id = ac.pipeline_id
                WHERE p.name = $1 AND p.tenant_id = $2
                GROUP BY p.id, rt.id
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
        program_id: Option<ProgramId>,
        pipline_name: &str,
        pipeline_description: &str,
        config: &RuntimeConfig,
        connectors: &Option<Vec<AttachedConnector>>,
    ) -> Result<(PipelineId, Version), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let new_pipeline = txn
            .prepare_cached(
                "INSERT INTO pipeline (id, program_id, version, name, description, config, tenant_id) VALUES($1, $2, 1, $3, $4, $5, $6)")
            .await?;
        let new_runtime_state = txn
            .prepare_cached(
                "INSERT INTO pipeline_runtime_state (id, tenant_id, desired_status, current_status, status_since, created) VALUES($1, $2, 'shutdown', 'shutdown', extract(epoch from now()), extract(epoch from now()))")
            .await?;

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
        program_id: Option<ProgramId>,
        pipline_name: &str,
        pipeline_description: &str,
        config: &Option<RuntimeConfig>,
        connectors: &Option<Vec<AttachedConnector>>,
    ) -> Result<Version, DBError> {
        log::trace!(
            "Updating config {} {} {} {} {:?} {:?}",
            pipeline_id.0,
            program_id
                .map(|pid| pid.0.to_string())
                .unwrap_or("<not set>".into()),
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

    async fn store_api_key_hash(
        &self,
        tenant_id: TenantId,
        key: String,
        scopes: Vec<ApiPermission>,
    ) -> Result<(), DBError> {
        let mut hasher = sha::Sha256::new();
        hasher.update(key.as_bytes());
        let hash = openssl::base64::encode_block(&hasher.finish());
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("INSERT INTO api_key (hash, tenant_id, scopes) VALUES ($1, $2, $3)")
            .await?;
        let res = manager
            .execute(
                &stmt,
                &[
                    &hash,
                    &tenant_id.0,
                    &scopes
                        .iter()
                        .map(|scope| match scope {
                            ApiPermission::Read => "read",
                            ApiPermission::Write => "write",
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
        api_key: String,
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
            .map(|s| {
                if s == "read" {
                    ApiPermission::Read
                } else {
                    ApiPermission::Write
                }
            })
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
        let program_id = row.get::<_, Option<Uuid>>(5).map(ProgramId);
        Ok(PipelineDescr {
            pipeline_id,
            program_id,
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
        let program_id = row.get::<_, Option<Uuid>>(5).map(ProgramId);
        let descriptor = PipelineDescr {
            pipeline_id,
            program_id,
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
        let pipeline = self
            .get_pipeline_descr_by_id(tenant_id, pipeline_id)
            .await?;
        let program_id = pipeline.program_id.ok_or(DBError::ProgramNotSet)?;
        let program = self.get_program_by_id(tenant_id, program_id, true).await?;
        let connectors = self
            .get_connectors_for_pipeline_id(tenant_id, pipeline_id)
            .await?;
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
            .get_pipeline_descr_by_id(tenant_id, pipeline_id)
            .await?;
        let connectors: Vec<ConnectorDescr> = self
            .get_connectors_for_pipeline_id(tenant_id, pipeline_id)
            .await?;
        PipelineRevision::generate_pipeline_config(&pipeline, &connectors)
    }

    async fn get_committed_program_by_id(
        &self,
        tenant_id: TenantId,
        program_id: ProgramId,
        revision: Revision,
    ) -> Result<ProgramDescr, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT
                name, description, version, status, error, schema, code
                FROM program_history WHERE id = $1 AND tenant_id = $2 AND revision = $3",
            )
            .await?;
        let row = manager
            .query_opt(&stmt, &[&program_id.0, &tenant_id.0, &revision.0])
            .await?;

        if let Some(row) = row {
            let name: String = row.get(0);
            let description: String = row.get(1);
            let version: Version = Version(row.get(2));
            let status: Option<String> = row.get(3);
            let error: Option<String> = row.get(4);
            let schema: Option<ProgramSchema> = row
                .get::<_, Option<String>>(5)
                .map(|s| serde_json::from_str(&s))
                .transpose()
                .map_err(|e| DBError::invalid_data(format!("Error parsing program schema: {e}")))?;
            let status = ProgramStatus::from_columns(status.as_deref(), error)?;
            let code = row.get(6);
            Ok(ProgramDescr {
                program_id,
                name,
                description,
                version,
                status,
                schema,
                code,
            })
        } else {
            Err(DBError::UnknownProgram { program_id })
        }
    }

    async fn get_committed_pipeline_by_id(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        revision: Revision,
    ) -> Result<PipelineDescr, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT p.id, version, p.name as cname, description, p.config, program_id,
                COALESCE(json_agg(json_build_object('name', ach.name,
                                                    'connector_id', connector_id,
                                                    'config', ach.config,
                                                    'is_input', is_input))
                                FILTER (WHERE ach.name IS NOT NULL),
                        '[]')
                FROM pipeline_history p
                LEFT JOIN attached_connector_history ach on p.id = ach.pipeline_id AND ach.revision = $3
                WHERE p.id = $1 AND p.tenant_id = $2 AND p.revision = $3
                GROUP BY p.id, p.version, p.name, p.description, p.config, p.program_id
                ")
            .await?;
        let row = manager
            .query_opt(&stmt, &[&pipeline_id.0, &tenant_id.0, &revision.0])
            .await?
            .ok_or(DBError::UnknownPipeline { pipeline_id })?;

        self.row_to_pipeline_descr(&row).await
    }

    async fn get_committed_connectors_by_id(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        revision: Revision,
    ) -> Result<Vec<ConnectorDescr>, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT ch.id, ch.name, ch.description, ch.config
            FROM connector_history ch, attached_connector_history ach
            WHERE ach.pipeline_id = $1 AND ach.connector_id = ch.id AND ch.tenant_id = $2 AND ch.revision = $3")
            .await?;

        let rows = manager
            .query(&stmt, &[&pipeline_id.0, &tenant_id.0, &revision.0])
            .await?;

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

    /// Retrieve all connectors referenced by a pipeline.
    async fn get_connectors_for_pipeline_id(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<Vec<ConnectorDescr>, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT c.id, c.name, c.description, c.config
            FROM connector c, attached_connector ac
            WHERE ac.pipeline_id = $1
            AND ac.connector_id = c.id
            AND c.tenant_id = $2",
            )
            .await?;

        let rows = manager
            .query(&stmt, &[&pipeline_id.0, &tenant_id.0])
            .await?;

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
            WHERE tenant_id = $1 AND id = $4")
        .await?;
        let rows = txn
            .execute(
                &stmt,
                &[
                    &tenant_id.0,
                    &ac.name,
                    &pipeline_id.0,
                    &ac.connector_id.0,
                    &ac.is_input,
                    &ac.relation_name,
                ],
            )
            .map_err(Self::maybe_unique_violation)
            .map_err(|e| Self::maybe_pipeline_id_foreign_key_constraint_err(e, pipeline_id))
            .await?;
        if rows == 0 {
            Err(DBError::UnknownConnector {
                connector_id: ac.connector_id,
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

            let uuid_str = obj.get("connector_id").unwrap().as_str().unwrap();
            let connector_id = ConnectorId(Uuid::parse_str(uuid_str).map_err(|e| {
                DBError::invalid_data(format!("error parsing connector id '{uuid_str}': {e}"))
            })?);

            attached_connectors.push(AttachedConnector {
                name: obj.get("name").unwrap().as_str().unwrap().to_owned(),
                connector_id,
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
                    Some("api_key_pkey") => DBError::duplicate_key(),
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
