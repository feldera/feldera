use std::{
    borrow::Cow,
    collections::{BTreeMap, HashSet},
    fmt::{self, Display},
};

use chrono::{DateTime, NaiveDateTime, Utc};
use deadpool_postgres::Transaction;
use futures_util::TryFutureExt;
use pipeline_types::{
    config::{InputEndpointConfig, OutputEndpointConfig, PipelineConfig, RuntimeConfig},
    error::ErrorResponse,
    query::OutputQuery,
};
use serde_json::Value;
use tokio_postgres::Row;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::auth::TenantId;

use super::{
    storage::Storage, ConnectorDescr, ConnectorId, DBError, ProgramDescr, ProgramId, ProjectDB,
    Version,
};
use crate::api::{ConnectorConfig, ConnectorConfigVariant, ServiceConfig};
use crate::db::{ServiceDescr, ServiceId};
use pipeline_types::program_schema::{canonical_identifier, Relation};
use serde::{Deserialize, Serialize};

/// Unique pipeline id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct PipelineId(
    #[cfg_attr(test, proptest(strategy = "super::test::limited_uuid()"))] pub Uuid,
);
impl Display for PipelineId {
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
    #[cfg_attr(test, proptest(strategy = "super::test::limited_uuid()"))] pub Uuid,
);

impl Display for AttachedConnectorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Revision number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct Revision(pub(crate) Uuid);
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
    /// The versioned services for each connector.
    pub(crate) services_for_connectors: Vec<Vec<ServiceDescr>>,
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
        services_for_connectors: Vec<Vec<ServiceDescr>>,
        program: ProgramDescr,
    ) -> Self {
        assert!(
            PipelineRevision::validate(&pipeline, &connectors, &services_for_connectors, &program)
                .is_ok(),
            "pre-condition: Validate supplied data is a consistent/valid snapshot"
        );
        // This unwrap() will succeed because the pre-conditions above make sure that
        // the config is valid
        let config = PipelineRevision::generate_pipeline_config(
            &pipeline,
            &connectors,
            &services_for_connectors,
        )
        .unwrap();

        PipelineRevision {
            revision,
            pipeline,
            connectors,
            services_for_connectors,
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
    /// - The `services_for_connectors` vector contains all services referenced
    ///   by the connectors in their configuration.
    /// - attached connectors only reference table/view names that exist in the
    ///   `program.schema`.
    pub(crate) fn validate(
        pipeline: &PipelineDescr,
        connectors: &[ConnectorDescr],
        services_for_connectors: &[Vec<ServiceDescr>],
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

        let tables = HashSet::<_>::from_iter(schema.inputs.iter().map(Relation::name));
        let acs_with_missing_tables: Vec<(String, String)> = pipeline
            .attached_connectors
            .iter()
            .filter(|ac| ac.is_input && !tables.contains(&canonical_identifier(&ac.relation_name)))
            .map(|ac| (ac.name.clone(), ac.relation_name.clone()))
            .collect();
        if !acs_with_missing_tables.is_empty() {
            return Err(DBError::TablesNotInSchema {
                missing: acs_with_missing_tables,
            });
        }

        let views = HashSet::<_>::from_iter(schema.outputs.iter().map(Relation::name));
        let acs_with_missing_views: Vec<(String, String)> = pipeline
            .attached_connectors
            .iter()
            .filter(|ac| !ac.is_input && !views.contains(&canonical_identifier(&ac.relation_name)))
            .map(|ac| (ac.name.clone(), ac.relation_name.clone()))
            .collect();
        if !acs_with_missing_views.is_empty() {
            return Err(DBError::ViewsNotInSchema {
                missing: acs_with_missing_views,
            });
        }

        assert_eq!(connectors.len(), services_for_connectors.len());
        for i in 0..connectors.len() {
            let connector = &connectors[i];
            let services = &services_for_connectors[i];
            assert_eq!(
                HashSet::<String>::from_iter(connector.config.service_names().iter().cloned()),
                HashSet::from_iter(services.iter().map(|s| s.name.clone())),
                "pre-condition: supplied all services necessary"
            );
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
        services_for_connectors: &[Vec<ServiceDescr>],
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
            let idx = connectors.iter().position(|c| ac.connector_name == c.name);
            match idx {
                None => {
                    return Err(DBError::UnknownConnectorName {
                        connector_name: ac.connector_name.clone(),
                    });
                }
                Some(idx) => {
                    let connector = connectors[idx].clone();
                    let input_endpoint_config = InputEndpointConfig {
                        stream: Cow::from(ac.relation_name.clone()),
                        connector_config: connector.config.to_pipeline_connector_config(
                            &services_array_to_map(&services_for_connectors[idx]),
                        )?,
                    };
                    expanded_inputs.insert(Cow::from(ac.name.clone()), input_endpoint_config);
                }
            }
        }
        let mut expanded_outputs: BTreeMap<Cow<'static, str>, OutputEndpointConfig> =
            BTreeMap::new();
        for ac in outputs.iter() {
            let idx = connectors.iter().position(|c| ac.connector_name == c.name);
            match idx {
                None => {
                    return Err(DBError::UnknownConnectorName {
                        connector_name: ac.connector_name.clone(),
                    });
                }
                Some(idx) => {
                    let connector = connectors[idx].clone();
                    let output_endpoint_config = OutputEndpointConfig {
                        stream: Cow::from(ac.relation_name.clone()),
                        // This field gets skipped during serialization/deserialization,
                        // so it doesn't matter what value we use here
                        query: OutputQuery::default(),
                        connector_config: connector.config.to_pipeline_connector_config(
                            &services_array_to_map(&services_for_connectors[idx]),
                        )?,
                    };
                    expanded_outputs.insert(Cow::from(ac.name.clone()), output_endpoint_config);
                }
            }
        }

        let pc = PipelineConfig {
            name: Some(format!("pipeline-{pipeline_id}")),
            global: pipeline.config.clone(),
            storage_location: None,
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
    /// The pipeline's ID
    pub pipeline_id: PipelineId,

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

pub(crate) async fn all_pipelines(db: &ProjectDB) -> Result<Vec<(TenantId, PipelineId)>, DBError> {
    let manager = db.pool.get().await?;
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

/// Resolve all references and deploy a new pipeline
pub(crate) async fn create_pipeline_deployment(
    db: &ProjectDB,
    deployment_id: Uuid,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
) -> Result<Revision, DBError> {
    let mut client = db.pool.get().await?;
    let txn = client.transaction().await?;
    let (pipeline, program, connectors, services_for_connectors) =
        is_pipeline_deployable(db, tenant_id, pipeline_id).await?;
    let pipeline_revision = PipelineRevision::new(
        Revision(deployment_id),
        pipeline,
        connectors,
        services_for_connectors,
        program,
    );

    let pipeline_config = serde_json::to_string(&pipeline_revision).unwrap();
    let check_deployment = txn
        .prepare_cached(
            "SELECT config FROM pipeline_deployment WHERE pipeline_id = $1 AND tenant_id = $2",
        )
        .await?;
    let row: Option<Row> = txn
        .query_opt(&check_deployment, &[&pipeline_id.0, &tenant_id.0])
        .await?;

    if let Some(row) = row {
        let current_revision: String = row.get(0);
        if pipeline_config == current_revision {
            return Err(DBError::RevisionNotChanged);
        }
    }
    let stmt = txn
            .prepare_cached("INSERT INTO pipeline_deployment (id, pipeline_id, tenant_id, config) VALUES ($1, $2, $3, $4)
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

pub(crate) async fn get_pipeline_deployment(
    db: &ProjectDB,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
) -> Result<PipelineRevision, DBError> {
    let manager = db.pool.get().await?;
    let stmt = manager
        .prepare_cached(
            "SELECT config FROM pipeline_deployment WHERE pipeline_id = $1 AND tenant_id = $2",
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

pub(crate) async fn list_pipelines(
    db: &ProjectDB,
    tenant_id: TenantId,
) -> Result<Vec<Pipeline>, DBError> {
    let manager = db.pool.get().await?;
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
        result.push(row_to_pipeline(&row)?);
    }

    Ok(result)
}

pub(crate) async fn get_pipeline_by_id(
    db: &ProjectDB,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
) -> Result<Pipeline, DBError> {
    let manager = db.pool.get().await?;
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

    row_to_pipeline(&row)
}

pub(crate) async fn get_pipeline_descr_by_id(
    db: &ProjectDB,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    txn: Option<&Transaction<'_>>,
) -> Result<PipelineDescr, DBError> {
    let query = "SELECT p.id, p.version, p.name as cname, p.description, p.config, program.name,
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
        let manager = db.pool.get().await?;
        let stmt = manager.prepare_cached(query).await?;

        manager
            .query_opt(&stmt, &[&pipeline_id.0, &tenant_id.0])
            .await?
            .ok_or(DBError::UnknownPipeline { pipeline_id })?
    };

    row_to_pipeline_descr(&row)
}

pub(crate) async fn get_pipeline_runtime_state_by_id(
    db: &ProjectDB,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
) -> Result<PipelineRuntimeState, DBError> {
    let manager = db.pool.get().await?;
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

    if let Some(row) = row {
        Ok(PipelineRuntimeState {
            pipeline_id,
            location: row.get::<_, Option<String>>(0).unwrap_or_default(),
            desired_status: row.get::<_, String>(1).try_into()?,
            current_status: row.get::<_, String>(2).try_into()?,
            status_since: convert_bigint_to_time(row.get(3))?,
            error: row
                .get::<_, Option<String>>(4)
                .map(|s| deserialize_error_response(pipeline_id, &s))
                .transpose()?,
            created: convert_bigint_to_time(row.get(5))?,
        })
    } else {
        Err(DBError::UnknownPipeline { pipeline_id })
    }
}

pub(crate) async fn get_pipeline_runtime_state_by_name(
    db: &ProjectDB,
    tenant_id: TenantId,
    pipeline_name: &str,
) -> Result<PipelineRuntimeState, DBError> {
    let manager = db.pool.get().await?;
    let stmt = manager
        .prepare_cached(
            "SELECT pr.location, pr.desired_status, pr.current_status, pr.status_since, pr.error, pr.created, pr.id
                FROM pipeline_runtime_state pr
                JOIN pipeline p
                    ON p.id = pr.id AND p.tenant_id = pr.tenant_id
                WHERE p.name = $1 AND p.tenant_id = $2",
        )
        .await?;

    let row = manager
        .query_opt(&stmt, &[&pipeline_name, &tenant_id.0])
        .await?;

    if let Some(row) = row {
        let pipeline_id: PipelineId = PipelineId(row.get(6));
        Ok(PipelineRuntimeState {
            pipeline_id,
            location: row.get::<_, Option<String>>(0).unwrap_or_default(),
            desired_status: row.get::<_, String>(1).try_into()?,
            current_status: row.get::<_, String>(2).try_into()?,
            status_since: convert_bigint_to_time(row.get(3))?,
            error: row
                .get::<_, Option<String>>(4)
                .map(|s| deserialize_error_response(pipeline_id, &s))
                .transpose()?,
            created: convert_bigint_to_time(row.get(5))?,
        })
    } else {
        Err(DBError::UnknownPipelineName {
            pipeline_name: pipeline_name.to_string(),
        })
    }
}

pub(crate) async fn get_pipeline_descr_by_name(
    db: &ProjectDB,
    tenant_id: TenantId,
    name: &str,
    txn: Option<&Transaction<'_>>,
) -> Result<PipelineDescr, DBError> {
    let query = "SELECT p.id, p.version, p.name as cname, p.description, p.config, prog.name,
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
                ";
    let row = if let Some(txn) = txn {
        let stmt = txn.prepare_cached(query).await?;
        txn.query_opt(&stmt, &[&name, &tenant_id.0]).await?
    } else {
        let manager = db.pool.get().await?;
        let stmt = manager.prepare_cached(query).await?;
        manager.query_opt(&stmt, &[&name, &tenant_id.0]).await?
    };

    let row = row.ok_or(DBError::UnknownPipelineName {
        pipeline_name: name.to_string(),
    })?;

    row_to_pipeline_descr(&row)
}

pub(crate) async fn get_pipeline_by_name(
    db: &ProjectDB,
    tenant_id: TenantId,
    name: &str,
) -> Result<Pipeline, DBError> {
    let manager = db.pool.get().await?;
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
        .ok_or(DBError::UnknownPipelineName {
            pipeline_name: name.to_string(),
        })?;

    row_to_pipeline(&row)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn new_pipeline(
    tenant_id: TenantId,
    id: Uuid,
    program_name: &Option<String>,
    pipline_name: &str,
    pipeline_description: &str,
    config: &RuntimeConfig,
    connectors: &Option<Vec<AttachedConnector>>,
    txn: &Transaction<'_>,
) -> Result<(PipelineId, Version), DBError> {
    let new_pipeline = txn
        .prepare_cached(
            "INSERT INTO pipeline (id, program_id, version, name, description, config, tenant_id) 
                 VALUES($1, $2, 1, $3, $4, $5, $6)",
        )
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
        ProjectDB::maybe_tenant_id_foreign_key_constraint_err(e, tenant_id, program_id.map(|e| e.0))
    })
    .map_err(|e| ProjectDB::maybe_program_id_not_found_foreign_key_constraint_err(e, program_id))?;

    txn.execute(&new_runtime_state, &[&id, &tenant_id.0])
        .await?;

    let pipeline_id = PipelineId(id);

    if let Some(connectors) = connectors {
        // Add the connectors.
        for ac in connectors {
            attach_connector(tenant_id, txn, pipeline_id, ac).await?;
        }
    }

    Ok((pipeline_id, Version(1)))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn update_pipeline(
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    program_name: &Option<String>,
    pipline_name: &str,
    pipeline_description: &str,
    config: &Option<RuntimeConfig>,
    connectors: &Option<Vec<AttachedConnector>>,
    txn: &Transaction<'_>,
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
    let find_pipeline_id = txn
        .prepare_cached("SELECT id FROM pipeline WHERE id = $1 AND tenant_id = $2")
        .await?;
    let get_program_id = txn
        .prepare_cached("SELECT id FROM program WHERE tenant_id = $1 AND name = $2")
        .await?;
    let delete_ac = txn
        .prepare_cached("DELETE FROM attached_connector WHERE pipeline_id = $1 AND tenant_id = $2")
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
            attach_connector(tenant_id, txn, pipeline_id, ac).await?;
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
pub(crate) async fn update_pipeline_runtime_state(
    db: &ProjectDB,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    state: &PipelineRuntimeState,
) -> Result<(), DBError> {
    let current_status: &'static str = state.current_status.into();
    let manager = db.pool.get().await?;
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
pub(crate) async fn set_pipeline_desired_status(
    db: &ProjectDB,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    desired_status: PipelineStatus,
) -> Result<(), DBError> {
    let desired_status: &'static str = desired_status.into();
    let manager = db.pool.get().await?;
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

/// Returns true if the connector of a given name is an input connector.
pub(crate) async fn attached_connector_is_input(
    db: &ProjectDB,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    name: &str,
) -> Result<bool, DBError> {
    let manager = db.pool.get().await?;
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

pub(crate) async fn delete_pipeline(
    db: &ProjectDB,
    tenant_id: TenantId,
    pipeline_name: &str,
) -> Result<(), DBError> {
    let manager = db.pool.get().await?;
    let stmt = manager
        .prepare_cached("DELETE FROM pipeline WHERE name = $1 AND tenant_id = $2")
        .await?;
    let res = manager
        .execute(&stmt, &[&pipeline_name, &tenant_id.0])
        .await?;
    if res > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownPipelineName {
            pipeline_name: pipeline_name.to_string(),
        })
    }
}

pub(crate) async fn is_pipeline_deployable(
    db: &ProjectDB,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
) -> Result<
    (
        PipelineDescr,
        ProgramDescr,
        Vec<ConnectorDescr>,
        Vec<Vec<ServiceDescr>>,
    ),
    DBError,
> {
    let mut client = db.pool.get().await?;
    let txn = client.transaction().await?;
    let pipeline = db
        .get_pipeline_descr_by_id(tenant_id, pipeline_id, Some(&txn))
        .await?;
    let program_name = pipeline
        .program_name
        .as_ref()
        .ok_or(DBError::ProgramNotSet)?;
    let program = db
        .get_program_by_name(tenant_id, program_name, true, Some(&txn))
        .await?;
    let connectors = get_connectors_for_pipeline_id(db, tenant_id, pipeline_id, Some(&txn)).await?;
    let services_for_connectors = get_services_for_connectors(&txn, tenant_id, &connectors).await?;
    txn.commit().await?;
    // Check that this configuration forms a valid snapshot
    PipelineRevision::validate(&pipeline, &connectors, &services_for_connectors, &program)?;
    Ok((pipeline, program, connectors, services_for_connectors))
}

pub(crate) async fn pipeline_config(
    db: &ProjectDB,
    tenant_id: TenantId,
    pipeline_name: &str,
) -> Result<PipelineConfig, DBError> {
    let mut client = db.pool.get().await?;
    let txn = client.transaction().await?;
    let pipeline = db
        .get_pipeline_descr_by_name(tenant_id, pipeline_name, Some(&txn))
        .await?;
    let connectors: Vec<ConnectorDescr> =
        get_connectors_for_pipeline_id(db, tenant_id, pipeline.pipeline_id, Some(&txn)).await?;
    let services_for_connectors = get_services_for_connectors(&txn, tenant_id, &connectors).await?;
    PipelineRevision::generate_pipeline_config(&pipeline, &connectors, &services_for_connectors)
}

/// Retrieve all connectors referenced by a pipeline.
async fn get_connectors_for_pipeline_id(
    db: &ProjectDB,
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
        let manager = db.pool.get().await?;
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

/// Retrieve all services attached to the provided connectors.
///
/// TODO: use a join instead of a double nested for-loop
async fn get_services_for_connectors(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    connectors: &Vec<ConnectorDescr>,
) -> Result<Vec<Vec<ServiceDescr>>, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT id, name, description, config, config_type
            FROM service
            WHERE name = $1
            AND tenant_id = $2",
        )
        .await?;

    let mut services_for_connectors = vec![];
    for connector in connectors {
        let mut services = vec![];
        for service_name in connector.config.service_names() {
            let row = txn.query_opt(&stmt, &[&service_name, &tenant_id.0]).await?;
            if let Some(row) = row {
                services.push(ServiceDescr {
                    service_id: ServiceId(row.get(0)),
                    name: row.get(1),
                    description: row.get(2),
                    config: ServiceConfig::from_yaml_str(row.get(3)),
                });
            } else {
                return Err(DBError::UnknownName { name: service_name });
            }
        }
        services_for_connectors.push(services)
    }
    Ok(services_for_connectors)
}

/// Converts an array of service descriptions to a mapping
/// of service name to description. The provided service
/// descriptions might have duplicates, in which the length
/// of the map result will be less than the input array.
fn services_array_to_map(services_array: &Vec<ServiceDescr>) -> BTreeMap<String, ServiceDescr> {
    let mut services_map = BTreeMap::new();
    for service in services_array {
        services_map.insert(service.name.clone(), service.clone());
    }
    services_map
}

/// Attach connector to the pipeline.
///
/// # Precondition
/// - A valid pipeline for `pipeline_id` must exist.
async fn attach_connector(
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
        .map_err(ProjectDB::maybe_unique_violation)
        .map_err(|e| ProjectDB::maybe_pipeline_id_foreign_key_constraint_err(e, pipeline_id))
        .await?;
    if rows == 0 {
        Err(DBError::UnknownConnectorName {
            connector_name: ac.connector_name.to_string(),
        })
    } else {
        Ok(())
    }
}

fn row_to_pipeline_descr(row: &Row) -> Result<PipelineDescr, DBError> {
    let pipeline_id = PipelineId(row.get(0));
    let program_name = row.get::<_, Option<String>>(5);
    Ok(PipelineDescr {
        pipeline_id,
        program_name,
        version: Version(row.get(1)),
        name: row.get(2),
        description: row.get(3),
        config: RuntimeConfig::from_yaml(row.get(4)),
        attached_connectors: json_to_attached_connectors(row.get(6)),
    })
}

fn row_to_pipeline(row: &Row) -> Result<Pipeline, DBError> {
    let pipeline_id = PipelineId(row.get(0));
    let program_name = row.get::<_, Option<String>>(5);
    let descriptor = PipelineDescr {
        pipeline_id,
        program_name,
        version: Version(row.get(1)),
        name: row.get(2),
        description: row.get(3),
        config: RuntimeConfig::from_yaml(row.get(4)),
        attached_connectors: json_to_attached_connectors(row.get(6)),
    };

    let state = PipelineRuntimeState {
        pipeline_id,
        location: row.get::<_, Option<String>>(7).unwrap_or_default(),
        desired_status: row.get::<_, String>(8).try_into()?,
        current_status: row.get::<_, String>(9).try_into()?,
        status_since: convert_bigint_to_time(row.get(10))?,
        error: row
            .get::<_, Option<String>>(11)
            .map(|s| deserialize_error_response(pipeline_id, &s))
            .transpose()?,
        created: convert_bigint_to_time(row.get(12))?,
    };

    Ok(Pipeline { descriptor, state })
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

fn json_to_attached_connectors(connectors_json: Value) -> Vec<AttachedConnector> {
    let connector_arr = connectors_json.as_array().unwrap();
    let mut attached_connectors = Vec::with_capacity(connector_arr.len());
    for connector in connector_arr {
        let obj = connector.as_object().unwrap();
        let is_input: bool = obj.get("is_input").unwrap().as_bool().unwrap();
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
    attached_connectors
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
