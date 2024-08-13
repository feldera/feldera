use crate::db::types::common::Version;
use crate::db::types::pipeline::{PipelineId, PipelineStatus};
use crate::db::types::program::ProgramStatus;
use crate::db::types::tenant::TenantId;
use actix_web::{
    body::BoxBody, http::StatusCode, HttpResponse, HttpResponseBuilder, ResponseError,
};
use deadpool_postgres::PoolError;
use log::Level;
use pipeline_types::error::DetailedError;
use pipeline_types::error::ErrorResponse;
use refinery::Error as RefineryError;
use serde::{ser::SerializeStruct, Serialize, Serializer};
use std::{backtrace::Backtrace, borrow::Cow, error::Error as StdError, fmt, fmt::Display};
use tokio_postgres::error::Error as PgError;

#[derive(Debug, Serialize)]
#[serde(untagged)]
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
    #[cfg(feature = "pg-embed")]
    #[serde(serialize_with = "serialize_pgembed_error")]
    PgEmbedError {
        error: Box<pg_embed::pg_errors::PgEmbedError>,
        backtrace: Backtrace,
    },
    // Catch-all error for unexpected invalid data extracted from the database.
    // This error is thrown when deserialization fails.
    #[serde(serialize_with = "serialize_invalid_data")]
    InvalidData {
        error: String,
        backtrace: Backtrace,
    },
    #[serde(serialize_with = "serialize_invalid_program_status")]
    InvalidProgramStatus {
        status: String,
        backtrace: Backtrace,
    },
    #[serde(serialize_with = "serialize_invalid_pipeline_status")]
    InvalidPipelineStatus {
        status: String,
        backtrace: Backtrace,
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
    CannotUpdateNonShutdownPipeline,
    CannotDeleteNonShutdownPipeline,
    CannotRenameNonExistingPipeline,
    OutdatedProgramVersion {
        latest_version: Version,
    },
    InvalidConnectorTransport {
        reason: String,
    },
    ProgramNotYetCompiled,
    ProgramFailedToCompile,
    InvalidProgramStatusTransition {
        current: ProgramStatus,
        transition_to: ProgramStatus,
    },
    InvalidDeploymentStatusTransition {
        current: PipelineStatus,
        transition_to: PipelineStatus,
    },
    IllegalPipelineStateTransition {
        hint: String,
        status: PipelineStatus,
        desired_status: PipelineStatus,
        requested_desired_status: PipelineStatus,
    },
}

impl DBError {
    pub fn invalid_data(error: String) -> Self {
        Self::InvalidData {
            error,
            backtrace: Backtrace::capture(),
        }
    }
    pub fn invalid_program_status(status: String) -> Self {
        Self::InvalidProgramStatus {
            status,
            backtrace: Backtrace::capture(),
        }
    }
    pub fn invalid_pipeline_status(status: String) -> Self {
        Self::InvalidPipelineStatus {
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

#[cfg(feature = "pg-embed")]
fn serialize_pgembed_error<S>(
    error: &pg_embed::pg_errors::PgEmbedError,
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

fn serialize_invalid_data<S>(
    error: &String,
    backtrace: &Backtrace,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("InvalidData", 2)?;
    ser.serialize_field("error", error)?;
    ser.serialize_field("backtrace", &backtrace.to_string())?;
    ser.end()
}

fn serialize_invalid_program_status<S>(
    error: &String,
    backtrace: &Backtrace,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("InvalidProgramStatus", 2)?;
    ser.serialize_field("error", error)?;
    ser.serialize_field("backtrace", &backtrace.to_string())?;
    ser.end()
}

fn serialize_invalid_pipeline_status<S>(
    status: &String,
    backtrace: &Backtrace,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("InvalidPipelineStatus", 2)?;
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

#[cfg(feature = "pg-embed")]
impl From<pg_embed::pg_errors::PgEmbedError> for DBError {
    fn from(error: pg_embed::pg_errors::PgEmbedError) -> Self {
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
            #[cfg(feature = "pg-embed")]
            DBError::PgEmbedError { error, .. } => {
                write!(f, "PG-embed error: '{error}'")
            }
            DBError::InvalidData { error, .. } => {
                write!(f, "Invalid database data '{error}'")
            }
            DBError::InvalidProgramStatus { status, .. } => {
                write!(f, "Invalid program status string '{status}'")
            }
            DBError::InvalidPipelineStatus { status, .. } => {
                write!(f, "Invalid pipeline status string '{status}'")
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
            DBError::CannotUpdateNonShutdownPipeline => {
                write!(f, "Cannot update a pipeline which is not fully shutdown. Shutdown the pipeline first by invoking the '/shutdown' endpoint.")
            }
            DBError::CannotDeleteNonShutdownPipeline => {
                write!(f, "Cannot delete a pipeline which is not fully shutdown. Shutdown the pipeline first by invoking the '/shutdown' endpoint.")
            }
            DBError::CannotRenameNonExistingPipeline => {
                write!(f, "The pipeline name in the request body does not match the one provided in the URL path. This is not allowed when no pipeline with the name provided in the URL path exists.")
            }
            DBError::OutdatedProgramVersion { latest_version } => {
                write!(
                    f,
                    "Outdated program version. Latest version: '{latest_version}'"
                )
            }
            DBError::InvalidConnectorTransport { reason } => {
                write!(f, "Invalid connector transport: '{reason}'")
            }
            DBError::ProgramNotYetCompiled => {
                write!(f, "The program hasn't been compiled yet")
            }
            DBError::ProgramFailedToCompile => {
                write!(f, "The program did not compile successfully")
            }
            DBError::InvalidProgramStatusTransition {
                current,
                transition_to,
            } => {
                write!(
                    f,
                    "Cannot transition from program status '{current:?}' to '{transition_to:?}'"
                )
            }
            DBError::InvalidDeploymentStatusTransition {
                current,
                transition_to,
            } => {
                write!(
                    f,
                    "Cannot transition from deployment status '{current:?}' to '{transition_to:?}'"
                )
            }
            DBError::IllegalPipelineStateTransition {
                hint,
                status,
                desired_status,
                requested_desired_status,
            } => {
                write!(
                    f,
                    "Deployment status (current: '{status:?}', desired: '{desired_status:?}') cannot have desired changed to '{requested_desired_status:?}'. {hint}"
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
            #[cfg(feature = "pg-embed")]
            Self::PgEmbedError { .. } => Cow::from("PgEmbedError"),
            Self::InvalidData { .. } => Cow::from("InvalidData"),
            Self::InvalidProgramStatus { .. } => Cow::from("InvalidProgramStatus"),
            Self::InvalidPipelineStatus { .. } => Cow::from("InvalidPipelineStatus"),
            Self::UniqueKeyViolation { .. } => Cow::from("UniqueKeyViolation"),
            Self::DuplicateKey { .. } => Cow::from("DuplicateKey"),
            Self::MissingMigrations { .. } => Cow::from("MissingMigrations"),
            Self::DuplicateName => Cow::from("DuplicateName"),
            Self::EmptyName => Cow::from("EmptyName"),
            Self::UnknownTenant { .. } => Cow::from("UnknownTenant"),
            Self::UnknownApiKey { .. } => Cow::from("UnknownApiKey"),
            Self::InvalidApiKey => Cow::from("InvalidApiKey"),
            Self::UnknownPipeline { .. } => Cow::from("UnknownPipeline"),
            Self::UnknownPipelineName { .. } => Cow::from("UnknownPipelineName"),
            Self::CannotUpdateNonShutdownPipeline { .. } => {
                Cow::from("CannotUpdateNonShutdownPipeline")
            }
            Self::CannotDeleteNonShutdownPipeline { .. } => {
                Cow::from("CannotDeleteNonShutdownPipeline")
            }
            Self::CannotRenameNonExistingPipeline { .. } => {
                Cow::from("CannotRenameNonExistingPipeline")
            }
            Self::OutdatedProgramVersion { .. } => Cow::from("OutdatedProgramVersion"),
            Self::InvalidConnectorTransport { .. } => Cow::from("InvalidConnectorTransport"),
            Self::ProgramNotYetCompiled { .. } => Cow::from("ProgramNotYetCompiled"),
            Self::ProgramFailedToCompile { .. } => Cow::from("ProgramFailedToCompile"),
            Self::InvalidProgramStatusTransition { .. } => {
                Cow::from("InvalidProgramStatusTransition")
            }
            Self::InvalidDeploymentStatusTransition { .. } => {
                Cow::from("InvalidDeploymentStatusTransition")
            }
            Self::IllegalPipelineStateTransition { .. } => {
                Cow::from("IllegalPipelineStateTransition")
            }
        }
    }

    fn log_level(&self) -> Level {
        match self {
            Self::UnknownPipeline { .. } => Level::Info,
            _ => Level::Error,
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
            #[cfg(feature = "pg-embed")]
            Self::PgEmbedError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidData { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidProgramStatus { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidPipelineStatus { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::UniqueKeyViolation { .. } => StatusCode::INTERNAL_SERVER_ERROR, // UUID conflict
            Self::DuplicateKey { .. } => StatusCode::INTERNAL_SERVER_ERROR, // This error should never bubble up till here
            Self::MissingMigrations { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::DuplicateName => StatusCode::CONFLICT,
            Self::EmptyName => StatusCode::BAD_REQUEST,
            Self::UnknownTenant { .. } => StatusCode::UNAUTHORIZED, // TODO: should we report not found instead?
            Self::UnknownApiKey { .. } => StatusCode::NOT_FOUND,
            Self::InvalidApiKey => StatusCode::UNAUTHORIZED,
            Self::UnknownPipeline { .. } => StatusCode::NOT_FOUND,
            Self::UnknownPipelineName { .. } => StatusCode::NOT_FOUND,
            Self::CannotUpdateNonShutdownPipeline { .. } => StatusCode::BAD_REQUEST,
            Self::CannotDeleteNonShutdownPipeline { .. } => StatusCode::BAD_REQUEST,
            Self::CannotRenameNonExistingPipeline { .. } => StatusCode::BAD_REQUEST,
            Self::OutdatedProgramVersion { .. } => StatusCode::CONFLICT,
            Self::InvalidConnectorTransport { .. } => StatusCode::BAD_REQUEST,
            Self::ProgramNotYetCompiled => StatusCode::BAD_REQUEST, // User trying to start a pipeline whose program has not yet finished compilation
            Self::ProgramFailedToCompile => StatusCode::BAD_REQUEST, // User trying to start a pipeline whose program failed to compile
            Self::InvalidProgramStatusTransition { .. } => StatusCode::INTERNAL_SERVER_ERROR, // Compiler error
            Self::InvalidDeploymentStatusTransition { .. } => StatusCode::INTERNAL_SERVER_ERROR, // Runner error
            Self::IllegalPipelineStateTransition { .. } => StatusCode::BAD_REQUEST, // Runner error
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponseBuilder::new(self.status_code()).json(ErrorResponse::from_error(self))
    }
}
