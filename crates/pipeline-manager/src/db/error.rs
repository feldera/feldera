use crate::db::types::pipeline::PipelineId;
use crate::db::types::program::ProgramStatus;
use crate::db::types::resources_status::{ResourcesDesiredStatus, ResourcesStatus};
use crate::db::types::storage::StorageStatus;
use crate::db::types::tenant::TenantId;
use crate::db::types::utils::ValidationError;
use crate::db::types::version::Version;
use actix_web::{
    body::BoxBody, http::StatusCode, HttpResponse, HttpResponseBuilder, ResponseError,
};
use deadpool_postgres::PoolError;
use feldera_types::error::DetailedError;
use feldera_types::error::ErrorResponse;
use refinery::Error as RefineryError;
use serde::{ser::SerializeStruct, Serialize, Serializer};
use std::{backtrace::Backtrace, borrow::Cow, error::Error as StdError, fmt, fmt::Display};
use tokio_postgres::error::Error as PgError;

#[derive(Debug, Serialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum DBError {
    #[serde(serialize_with = "serialize_pg_error")]
    PostgresError {
        error: Box<PgError>,
        backtrace: Backtrace,
    },
    #[serde(serialize_with = "serialize_pgpool_error")]
    PostgresPoolError {
        error: Box<PoolError>,
        backtrace: Backtrace,
    },
    #[serde(serialize_with = "serialize_refinery_error")]
    PostgresMigrationError {
        error: Box<RefineryError>,
        backtrace: Backtrace,
    },
    #[cfg(feature = "postgresql_embedded")]
    #[serde(serialize_with = "serialize_pgembed_error")]
    PgEmbedError {
        error: Box<postgresql_embedded::Error>,
        backtrace: Backtrace,
    },
    #[serde(serialize_with = "serialize_invalid_program_status")]
    InvalidProgramStatus {
        status: String,
        backtrace: Backtrace,
    },
    #[serde(serialize_with = "serialize_invalid_storage_status")]
    InvalidStorageStatus {
        status: String,
        backtrace: Backtrace,
    },
    InvalidJsonData {
        data: String,
        error: String,
    },
    InvalidRuntimeConfig {
        value: serde_json::Value,
        error: ValidationError,
    },
    InvalidProgramConfig {
        value: serde_json::Value,
        error: ValidationError,
    },
    InvalidProgramInfo {
        value: serde_json::Value,
        error: ValidationError,
    },
    InvalidDeploymentConfig {
        value: serde_json::Value,
        error: ValidationError,
    },
    InvalidProgramError {
        value: serde_json::Value,
        error: String,
    },
    EditRestrictedToClearedStorage {
        not_allowed: Vec<String>,
    },
    InvalidErrorResponse {
        value: serde_json::Value,
        error: String,
    },
    FailedToSerializeRuntimeConfig {
        error: String,
    },
    FailedToSerializeProgramConfig {
        error: String,
    },
    FailedToSerializeProgramError {
        error: String,
    },
    FailedToSerializeErrorResponse {
        error: String,
    },
    #[serde(serialize_with = "serialize_unique_key_violation")]
    UniqueKeyViolation {
        constraint: &'static str,
        backtrace: Backtrace,
    },
    #[serde(serialize_with = "serialize_duplicate_key")]
    DuplicateKey {
        backtrace: Backtrace,
    },
    // General errors
    MissingMigrations {
        expected: u32,
        actual: u32,
    },
    DuplicateName, // When a database unique name constraint is violated
    EmptyName,
    TooLongName {
        name: String,
        length: usize,
        maximum: usize,
    },
    NameDoesNotMatchPattern {
        name: String,
    },
    // Tenant-related errors
    UnknownTenant {
        tenant_id: TenantId,
    },
    // API key-related errors
    UnknownApiKey {
        name: String,
    },
    InvalidApiKey,
    // Pipeline-related errors
    UnknownPipeline {
        pipeline_id: PipelineId,
    },
    UnknownPipelineName {
        pipeline_name: String,
    },
    UpdateRestrictedToStopped,
    ProgramStatusUpdateRestrictedToStopped,
    DeleteRestrictedToFullyStopped,
    DeleteRestrictedToClearedStorage,
    CannotRenameNonExistingPipeline,
    OutdatedProgramVersion {
        outdated_version: Version,
        latest_version: Version,
    },
    OutdatedPipelineVersion {
        outdated_version: Version,
        latest_version: Version,
    },
    StartFailedDueToFailedCompilation {
        compiler_error: String,
    },
    TransitionRequiresCompiledProgram {
        current: ResourcesStatus,
        transition_to: ResourcesStatus,
    },
    InvalidProgramStatusTransition {
        current_status: ProgramStatus,
        new_status: ProgramStatus,
    },
    InvalidResourcesStatusTransition {
        storage_status: StorageStatus,
        current_status: ResourcesStatus,
        new_status: ResourcesStatus,
    },
    InvalidStorageStatusTransition {
        current_status: StorageStatus,
        new_status: StorageStatus,
    },
    StorageStatusImmutableUnlessStopped {
        resources_status: ResourcesStatus,
        current_status: StorageStatus,
        new_status: StorageStatus,
    },
    IllegalPipelineAction {
        status: ResourcesStatus,
        current_desired_status: ResourcesDesiredStatus,
        new_desired_status: ResourcesDesiredStatus,
        hint: String,
    },
    TlsConnection {
        hint: String,
        #[serde(skip)]
        openssl_error: Option<openssl::error::ErrorStack>,
    },
    PauseWhileNotProvisioned,
    ResumeWhileNotProvisioned,
    InvalidResourcesStatus(String),
    InvalidResourcesDesiredStatus(String),
    InvalidRuntimeStatus(String),
    InvalidRuntimeDesiredStatus(String),
    NoRuntimeStatusWhileProvisioned,
    PreconditionViolation(String),
    InitialImmutableUnlessStopped,
    InitialStandbyNotAllowed,
}

impl DBError {
    pub fn invalid_program_status(status: String) -> Self {
        Self::InvalidProgramStatus {
            status,
            backtrace: Backtrace::capture(),
        }
    }
    pub fn invalid_storage_status(status: String) -> Self {
        Self::InvalidStorageStatus {
            status,
            backtrace: Backtrace::capture(),
        }
    }
    pub fn unique_key_violation(constraint: &'static str) -> Self {
        Self::UniqueKeyViolation {
            constraint,
            backtrace: Backtrace::capture(),
        }
    }
    pub fn duplicate_key() -> Self {
        Self::DuplicateKey {
            backtrace: Backtrace::capture(),
        }
    }
}

fn serialize_pg_error<S>(
    error: &PgError,
    backtrace: &Backtrace,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("PgError", 2)?;
    ser.serialize_field("error", &error.to_string())?;
    ser.serialize_field("backtrace", &backtrace.to_string())?;
    ser.end()
}

fn serialize_pgpool_error<S>(
    error: &PoolError,
    backtrace: &Backtrace,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("PgPoolError", 2)?;
    ser.serialize_field("error", &error.to_string())?;
    ser.serialize_field("backtrace", &backtrace.to_string())?;
    ser.end()
}

fn serialize_refinery_error<S>(
    error: &RefineryError,
    backtrace: &Backtrace,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("RefineryError", 2)?;
    ser.serialize_field("error", &error.to_string())?;
    ser.serialize_field("backtrace", &backtrace.to_string())?;
    ser.end()
}

#[cfg(feature = "postgresql_embedded")]
fn serialize_pgembed_error<S>(
    error: &postgresql_embedded::Error,
    backtrace: &Backtrace,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("PgEmbedError", 2)?;
    ser.serialize_field("error", &error.to_string())?;
    ser.serialize_field("backtrace", &backtrace.to_string())?;
    ser.end()
}

fn serialize_invalid_program_status<S>(
    status: &String,
    backtrace: &Backtrace,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("InvalidProgramStatus", 2)?;
    ser.serialize_field("status", status)?;
    ser.serialize_field("backtrace", &backtrace.to_string())?;
    ser.end()
}

fn serialize_invalid_storage_status<S>(
    status: &String,
    backtrace: &Backtrace,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("InvalidStorageStatus", 2)?;
    ser.serialize_field("status", &status.to_string())?;
    ser.serialize_field("backtrace", &backtrace.to_string())?;
    ser.end()
}

fn serialize_unique_key_violation<S>(
    constraint: &&str,
    backtrace: &Backtrace,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("UniqueKeyViolation", 2)?;
    ser.serialize_field("constraint", constraint)?;
    ser.serialize_field("backtrace", &backtrace.to_string())?;
    ser.end()
}

fn serialize_duplicate_key<S>(backtrace: &Backtrace, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("DuplicateKey", 1)?;
    ser.serialize_field("backtrace", &backtrace.to_string())?;
    ser.end()
}

impl From<PgError> for DBError {
    fn from(error: PgError) -> Self {
        Self::PostgresError {
            error: Box::new(error),
            backtrace: Backtrace::capture(),
        }
    }
}

impl From<PoolError> for DBError {
    fn from(error: PoolError) -> Self {
        Self::PostgresPoolError {
            error: Box::new(error),
            backtrace: Backtrace::capture(),
        }
    }
}

impl From<RefineryError> for DBError {
    fn from(error: RefineryError) -> Self {
        Self::PostgresMigrationError {
            error: Box::new(error),
            backtrace: Backtrace::capture(),
        }
    }
}

#[cfg(feature = "postgresql_embedded")]
impl From<postgresql_embedded::Error> for DBError {
    fn from(error: postgresql_embedded::Error) -> Self {
        Self::PgEmbedError {
            error: Box::new(error),
            backtrace: Backtrace::capture(),
        }
    }
}

impl Display for DBError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DBError::PostgresError { error, .. } => {
                write!(f, "Unexpected Postgres error: '{error}'")
            }
            DBError::PostgresPoolError { error, .. } => {
                write!(f, "Postgres connection pool error: '{error}'")
            }
            DBError::PostgresMigrationError { error, .. } => {
                write!(f, "DB schema migration error: '{error}'")
            }
            #[cfg(feature = "postgresql_embedded")]
            DBError::PgEmbedError { error, .. } => {
                write!(f, "PG-embed error: '{error}'")
            }
            DBError::InvalidProgramStatus { status, .. } => {
                write!(f, "String '{status}' is not a valid program status")
            }
            DBError::InvalidStorageStatus { status, .. } => {
                write!(f, "String '{status}' is not a valid storage status")
            }
            DBError::InvalidJsonData { data, error, .. } => {
                write!(
                    f,
                    "String data:\n{data}\n\n... is not valid JSON due to: {error}"
                )
            }
            DBError::InvalidRuntimeConfig { value, error } => {
                write!(
                    f,
                    "JSON for 'runtime_config' field:\n{value:#}\n\n... is not valid due to: {error}"
                )
            }
            DBError::InvalidProgramConfig { value, error } => {
                write!(
                    f,
                    "JSON for 'program_config' field:\n{value:#}\n\n... is not valid due to: {error}"
                )
            }
            DBError::InvalidProgramInfo { value, error } => {
                write!(
                    f,
                    "JSON for 'program_info' field:\n{value:#}\n\n... is not valid due to: {error}"
                )
            }
            DBError::InvalidDeploymentConfig { value, error } => {
                write!(
                    f,
                    "JSON for 'deployment_config' field:\n{value:#}\n\n... is not valid due to: {error}"
                )
            }
            DBError::InvalidProgramError { value, error } => {
                write!(f, "JSON for 'program_error' field:\n{value:#}\n\n... is not valid due to: {error}")
            }
            DBError::EditRestrictedToClearedStorage { not_allowed } => {
                write!(
                    f,
                    "The following pipeline edits are not allowed while storage is not cleared: {}",
                    not_allowed.join(", ")
                )
            }
            DBError::InvalidErrorResponse { value, error } => {
                write!(f, "JSON for 'deployment_error' field:\n{value:#}\n\n... is not valid due to: {error}")
            }
            DBError::FailedToSerializeRuntimeConfig { error } => {
                write!(f, "Unable to serialize runtime configuration for 'runtime_config' field as JSON due to: {error}")
            }
            DBError::FailedToSerializeProgramConfig { error } => {
                write!(f, "Unable to serialize program configuration for 'program_config' field as JSON due to: {error}")
            }
            DBError::FailedToSerializeProgramError { error } => {
                write!(f, "Unable to serialize program error for 'program_error' field as JSON due to: {error}")
            }
            DBError::FailedToSerializeErrorResponse { error } => {
                write!(f, "Unable to serialize error response for 'deployment_error' field as JSON due to: {error}")
            }
            DBError::UniqueKeyViolation { constraint, .. } => {
                write!(f, "Unique key violation for '{constraint}'")
            }
            DBError::DuplicateKey { .. } => {
                write!(f, "A key with the same hash already exists")
            }
            DBError::MissingMigrations { expected, actual } => {
                write!(
                    f,
                    "Expected database migrations to be applied up to {expected}, but database only has applied migrations up to {actual}"
                )
            }
            DBError::DuplicateName => {
                write!(f, "An entity with this name already exists")
            }
            DBError::EmptyName => {
                write!(f, "Name cannot be be empty")
            }
            DBError::TooLongName {
                name,
                length,
                maximum,
            } => {
                write!(
                    f,
                    "Name '{name}' is longer ({length}) than maximum allowed ({maximum})"
                )
            }
            DBError::NameDoesNotMatchPattern { name } => {
                write!(f, "Name '{name}' contains characters which are not lowercase (a-z), uppercase (A-Z), numbers (0-9), underscores (_) or hyphens (-)")
            }
            DBError::UnknownTenant { tenant_id } => {
                write!(f, "Unknown tenant id '{tenant_id}'")
            }
            DBError::UnknownApiKey { name } => {
                write!(f, "Unknown API key '{name}'")
            }
            DBError::InvalidApiKey => {
                write!(f, "Invalid API key")
            }
            DBError::UnknownPipeline { pipeline_id } => {
                write!(f, "Unknown pipeline id '{pipeline_id}'")
            }
            DBError::UnknownPipelineName { pipeline_name } => {
                write!(f, "Unknown pipeline name '{pipeline_name}'")
            }
            DBError::UpdateRestrictedToStopped => {
                write!(f, "Pipeline can only be updated while stopped. Stop it first by invoking '/stop'.")
            }
            DBError::ProgramStatusUpdateRestrictedToStopped => {
                write!(f, "Program status can only be updated while stopped.")
            }
            DBError::DeleteRestrictedToFullyStopped => {
                write!(f, "Cannot delete a pipeline which is not fully stopped. Stop the pipeline first fully by invoking the '/stop' endpoint.")
            }
            DBError::CannotRenameNonExistingPipeline => {
                write!(f, "The pipeline name in the request body does not match the one provided in the URL path. This is not allowed when no pipeline with the name provided in the URL path exists.")
            }
            DBError::OutdatedProgramVersion {
                outdated_version,
                latest_version,
            } => {
                write!(
                    f,
                    "Program version ({outdated_version}) is outdated by latest ({latest_version})"
                )
            }
            DBError::OutdatedPipelineVersion {
                outdated_version,
                latest_version,
            } => {
                write!(
                    f,
                    "Pipeline version ({outdated_version}) is outdated by latest ({latest_version})"
                )
            }
            DBError::StartFailedDueToFailedCompilation { .. } => {
                write!(
                    f,
                    "Not possible to start the pipeline because the program failed to compile"
                )
            }
            DBError::TransitionRequiresCompiledProgram {
                current,
                transition_to,
            } => {
                write!(
                    f,
                    "Transition from '{current:?}' to '{transition_to:?}' requires a successfully compiled program"
                )
            }
            DBError::InvalidProgramStatusTransition {
                current_status,
                new_status,
            } => {
                write!(
                    f,
                    "Cannot transition from program status '{current_status}' to '{new_status}'"
                )
            }
            DBError::InvalidResourcesStatusTransition {
                storage_status,
                current_status,
                new_status,
            } => {
                write!(
                    f,
                    "Cannot transition from deployment resources status '{current_status}' (storage status: '{storage_status}') to '{new_status}'"
                )
            }
            DBError::InvalidStorageStatusTransition {
                current_status,
                new_status,
            } => {
                write!(
                    f,
                    "Cannot transition from storage status '{current_status}' to '{new_status}'"
                )
            }
            DBError::StorageStatusImmutableUnlessStopped {
                resources_status,
                current_status,
                new_status,
            } => {
                write!(
                    f,
                    "Cannot transition storage status from '{current_status}' to '{new_status}' with resources status '{resources_status}'. \
                    Storage status cannot be changed unless the pipeline is stopped."
                )
            }
            DBError::IllegalPipelineAction {
                status,
                current_desired_status,
                new_desired_status,
                hint,
            } => {
                write!(
                    f,
                    "Deployment resources status (current: '{status:?}', desired: '{current_desired_status:?}') cannot have desired changed to '{new_desired_status:?}'. {hint}"
                )
            }
            DBError::PauseWhileNotProvisioned => {
                write!(
                    f,
                    "You can only call /pause when the pipeline is not Stopped, Provisioning or Stopping."
                )
            }
            DBError::ResumeWhileNotProvisioned => {
                write!(
                    f,
                    "You can only call /resume when the pipeline is not Stopped, Provisioning or Stopping."
                )
            }
            DBError::InvalidResourcesStatus(s) => {
                write!(f, "Invalid resources status: '{s}'")
            }
            DBError::InvalidResourcesDesiredStatus(s) => {
                write!(f, "Invalid resources desired status: '{s}'")
            }
            DBError::InvalidRuntimeStatus(s) => {
                write!(f, "Invalid runtime status: '{s}'")
            }
            DBError::InvalidRuntimeDesiredStatus(s) => {
                write!(f, "Invalid runtime desired status: '{s}'")
            }
            DBError::NoRuntimeStatusWhileProvisioned => {
                write!(
                    f,
                    "Runtime status is not set while the resources status is Provisioned"
                )
            }
            DBError::PreconditionViolation(s) => {
                write!(f, "Operation precondition not met: '{s}'")
            }
            DBError::TlsConnection { hint, .. } => {
                write!(f, "Unable to setup TLS connection to the database: {hint}")
            }
            DBError::DeleteRestrictedToClearedStorage => {
                write!(
                    f,
                    "Cannot delete pipeline unless its storage is cleared. Clear storage first using `/clear`."
                )
            }
            DBError::InitialImmutableUnlessStopped => {
                write!(
                    f,
                    "Initial desired runtime status cannot be changed unless the pipeline is Stopped."
                )
            }
            DBError::InitialStandbyNotAllowed => {
                write!(
                    f,
                    "Initial desired runtime status can only be set to Standby if: \
                    (1) `runtime_config.storage.backend.name` is set to `file`, and \
                    (2) `runtime_config.storage.backend.config.sync` is configured."
                )
            }
        }
    }
}

impl DetailedError for DBError {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::PostgresError { .. } => Cow::from("PostgresError"),
            Self::PostgresPoolError { .. } => Cow::from("PostgresPoolError"),
            Self::PostgresMigrationError { .. } => Cow::from("PostgresMigrationError"),
            #[cfg(feature = "postgresql_embedded")]
            Self::PgEmbedError { .. } => Cow::from("PgEmbedError"),
            Self::InvalidProgramStatus { .. } => Cow::from("InvalidProgramStatus"),
            Self::InvalidStorageStatus { .. } => Cow::from("InvalidStorageStatus"),
            Self::InvalidJsonData { .. } => Cow::from("InvalidJsonData"),
            Self::InvalidRuntimeConfig { .. } => Cow::from("InvalidRuntimeConfig"),
            Self::InvalidProgramConfig { .. } => Cow::from("InvalidProgramConfig"),
            Self::InvalidProgramInfo { .. } => Cow::from("InvalidProgramInfo"),
            Self::InvalidDeploymentConfig { .. } => Cow::from("InvalidDeploymentConfig"),
            Self::InvalidProgramError { .. } => Cow::from("InvalidProgramError"),
            Self::EditRestrictedToClearedStorage { .. } => {
                Cow::from("EditRestrictedToClearedStorage")
            }
            Self::InvalidErrorResponse { .. } => Cow::from("InvalidErrorResponse"),
            Self::FailedToSerializeRuntimeConfig { .. } => {
                Cow::from("FailedToSerializeRuntimeConfig")
            }
            Self::FailedToSerializeProgramConfig { .. } => {
                Cow::from("FailedToSerializeProgramConfig")
            }
            Self::FailedToSerializeProgramError { .. } => {
                Cow::from("FailedToSerializeProgramError")
            }
            Self::FailedToSerializeErrorResponse { .. } => {
                Cow::from("FailedToSerializeErrorResponse")
            }
            Self::UniqueKeyViolation { .. } => Cow::from("UniqueKeyViolation"),
            Self::DuplicateKey { .. } => Cow::from("DuplicateKey"),
            Self::MissingMigrations { .. } => Cow::from("MissingMigrations"),
            Self::DuplicateName => Cow::from("DuplicateName"),
            Self::EmptyName => Cow::from("EmptyName"),
            Self::TooLongName { .. } => Cow::from("TooLongName"),
            Self::NameDoesNotMatchPattern { .. } => Cow::from("NameDoesNotMatchPattern"),
            Self::UnknownTenant { .. } => Cow::from("UnknownTenant"),
            Self::UnknownApiKey { .. } => Cow::from("UnknownApiKey"),
            Self::InvalidApiKey => Cow::from("InvalidApiKey"),
            Self::UnknownPipeline { .. } => Cow::from("UnknownPipeline"),
            Self::UnknownPipelineName { .. } => Cow::from("UnknownPipelineName"),
            Self::UpdateRestrictedToStopped { .. } => Cow::from("UpdateRestrictedToStopped"),
            Self::ProgramStatusUpdateRestrictedToStopped { .. } => {
                Cow::from("ProgramStatusUpdateRestrictedToStopped")
            }
            Self::DeleteRestrictedToFullyStopped { .. } => {
                Cow::from("DeleteRestrictedToFullyStopped")
            }
            Self::CannotRenameNonExistingPipeline { .. } => {
                Cow::from("CannotRenameNonExistingPipeline")
            }
            Self::OutdatedProgramVersion { .. } => Cow::from("OutdatedProgramVersion"),
            Self::OutdatedPipelineVersion { .. } => Cow::from("OutdatedPipelineVersion"),
            Self::StartFailedDueToFailedCompilation { .. } => {
                Cow::from("StartFailedDueToFailedCompilation")
            }
            Self::TransitionRequiresCompiledProgram { .. } => {
                Cow::from("TransitionRequiresCompiledProgram")
            }
            Self::InvalidProgramStatusTransition { .. } => {
                Cow::from("InvalidProgramStatusTransition")
            }
            Self::InvalidResourcesStatusTransition { .. } => {
                Cow::from("InvalidResourcesStatusTransition")
            }
            Self::InvalidStorageStatusTransition { .. } => {
                Cow::from("InvalidStorageStatusTransition")
            }
            Self::StorageStatusImmutableUnlessStopped { .. } => {
                Cow::from("StorageStatusImmutableUnlessStopped")
            }
            Self::IllegalPipelineAction { .. } => Cow::from("IllegalPipelineAction"),
            Self::TlsConnection { .. } => Cow::from("TlsConnection"),
            Self::DeleteRestrictedToClearedStorage => Cow::from("DeleteRestrictedToClearedStorage"),
            Self::PauseWhileNotProvisioned => Cow::from("PauseWhileNotProvisioned"),
            Self::InvalidResourcesStatus(..) => Cow::from("InvalidResourcesStatus"),
            Self::InvalidResourcesDesiredStatus(..) => Cow::from("InvalidResourcesDesiredStatus"),
            Self::InvalidRuntimeStatus(..) => Cow::from("InvalidRuntimeStatus"),
            Self::InvalidRuntimeDesiredStatus(..) => Cow::from("InvalidRuntimeDesiredStatus"),
            Self::NoRuntimeStatusWhileProvisioned => Cow::from("NoRuntimeStatusWhileProvisioned"),
            Self::PreconditionViolation(..) => Cow::from("PreconditionViolation"),
            Self::ResumeWhileNotProvisioned => Cow::from("ResumeWhileNotProvisioned"),
            Self::InitialImmutableUnlessStopped => Cow::from("InitialImmutableUnlessStopped"),
            Self::InitialStandbyNotAllowed => Cow::from("InitialStandbyNotAllowed"),
        }
    }
}

impl StdError for DBError {}

impl ResponseError for DBError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::PostgresError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PostgresPoolError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PostgresMigrationError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            #[cfg(feature = "postgresql_embedded")]
            Self::PgEmbedError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidProgramStatus { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidStorageStatus { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidJsonData { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidRuntimeConfig { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidProgramConfig { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidProgramInfo { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidDeploymentConfig { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidProgramError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::EditRestrictedToClearedStorage { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidErrorResponse { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::FailedToSerializeRuntimeConfig { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::FailedToSerializeProgramConfig { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::FailedToSerializeProgramError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::FailedToSerializeErrorResponse { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::UniqueKeyViolation { .. } => StatusCode::INTERNAL_SERVER_ERROR, // UUID conflict
            Self::DuplicateKey { .. } => StatusCode::INTERNAL_SERVER_ERROR, // This error should never bubble up till here
            Self::MissingMigrations { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::DuplicateName => StatusCode::CONFLICT,
            Self::EmptyName => StatusCode::BAD_REQUEST,
            Self::TooLongName { .. } => StatusCode::BAD_REQUEST,
            Self::NameDoesNotMatchPattern { .. } => StatusCode::BAD_REQUEST,
            Self::UnknownTenant { .. } => StatusCode::UNAUTHORIZED, // TODO: should we report not found instead?
            Self::UnknownApiKey { .. } => StatusCode::NOT_FOUND,
            Self::InvalidApiKey => StatusCode::UNAUTHORIZED,
            Self::UnknownPipeline { .. } => StatusCode::NOT_FOUND,
            Self::UnknownPipelineName { .. } => StatusCode::NOT_FOUND,
            Self::UpdateRestrictedToStopped { .. } => StatusCode::BAD_REQUEST,
            Self::ProgramStatusUpdateRestrictedToStopped { .. } => StatusCode::BAD_REQUEST,
            Self::DeleteRestrictedToFullyStopped { .. } => StatusCode::BAD_REQUEST,
            Self::CannotRenameNonExistingPipeline { .. } => StatusCode::BAD_REQUEST,
            Self::OutdatedProgramVersion { .. } => StatusCode::CONFLICT,
            Self::OutdatedPipelineVersion { .. } => StatusCode::CONFLICT,
            Self::StartFailedDueToFailedCompilation { .. } => StatusCode::BAD_REQUEST,
            Self::TransitionRequiresCompiledProgram { .. } => StatusCode::INTERNAL_SERVER_ERROR, // Runner error
            Self::InvalidProgramStatusTransition { .. } => StatusCode::INTERNAL_SERVER_ERROR, // Compiler error
            Self::InvalidResourcesStatusTransition { .. } => StatusCode::INTERNAL_SERVER_ERROR, // Runner error
            Self::StorageStatusImmutableUnlessStopped { .. } => StatusCode::BAD_REQUEST,
            Self::IllegalPipelineAction { .. } => StatusCode::BAD_REQUEST, // User trying to set a deployment desired status which cannot be performed currently
            Self::TlsConnection { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::DeleteRestrictedToClearedStorage => StatusCode::BAD_REQUEST,
            Self::InvalidStorageStatusTransition { .. } => StatusCode::BAD_REQUEST,
            Self::PauseWhileNotProvisioned => StatusCode::BAD_REQUEST,
            Self::ResumeWhileNotProvisioned => StatusCode::BAD_REQUEST,
            Self::InvalidResourcesStatus(..) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidResourcesDesiredStatus(..) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidRuntimeStatus(..) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidRuntimeDesiredStatus(..) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::NoRuntimeStatusWhileProvisioned => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PreconditionViolation(..) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InitialImmutableUnlessStopped => StatusCode::BAD_REQUEST,
            Self::InitialStandbyNotAllowed => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponseBuilder::new(self.status_code()).json(ErrorResponse::from_error(self))
    }
}
