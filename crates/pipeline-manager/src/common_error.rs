use actix_web::{
    body::BoxBody, http::StatusCode, HttpResponse, HttpResponseBuilder, ResponseError,
};
use feldera_types::error::{DetailedError, ErrorResponse};
use serde::Serialize;
use serde::{ser::SerializeStruct, Serializer};
use std::backtrace::Backtrace;
use std::io::Error as IOError;
use std::{borrow::Cow, error::Error as StdError, fmt, fmt::Display};

/// The [`CommonError`] encompasses errors which commonly occur for which
/// a simple context parameter can be provided to indicate during what operation
/// it happened. These common errors are generally intended for when internal errors
/// occurred that should not happen. For errors caused by users (e.g., a user-provided
/// JSON is deserialized from after being given as body in an API) a dedicated error
/// variant should be created elsewhere (e.g., within [`crate::api::error::ApiError`]).
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum CommonError {
    #[serde(serialize_with = "serialize_io_error")]
    IoError {
        context: String,
        io_error: IOError,
        backtrace: Backtrace,
    },
    #[serde(serialize_with = "serialize_json_serialization_error")]
    JsonSerializationError {
        context: String,
        serde_error: serde_json::Error,
        backtrace: Backtrace,
    },
    #[serde(serialize_with = "serialize_json_deserialization_error")]
    JsonDeserializationError {
        context: String,
        serde_error: serde_json::Error,
        backtrace: Backtrace,
    },
    EnterpriseFeature {
        feature: String,
    },
}

impl CommonError {
    pub fn io_error(context: String, io_error: IOError) -> Self {
        Self::IoError {
            context,
            io_error,
            backtrace: Backtrace::capture(),
        }
    }
    pub fn json_serialization_error(context: String, serde_error: serde_json::Error) -> Self {
        Self::JsonSerializationError {
            context,
            serde_error,
            backtrace: Backtrace::capture(),
        }
    }
    pub fn json_deserialization_error(context: String, serde_error: serde_json::Error) -> Self {
        Self::JsonDeserializationError {
            context,
            serde_error,
            backtrace: Backtrace::capture(),
        }
    }
}

fn serialize_io_error<S>(
    context: &String,
    io_error: &IOError,
    backtrace: &Backtrace,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("IoError", 4)?;
    ser.serialize_field("context", &context)?;
    ser.serialize_field("kind", &io_error.kind().to_string())?;
    ser.serialize_field("os_error", &io_error.raw_os_error())?;
    ser.serialize_field("backtrace", &backtrace.to_string())?;
    ser.end()
}

fn serialize_json_serialization_error<S>(
    context: &String,
    serde_error: &serde_json::Error,
    backtrace: &Backtrace,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("JsonSerializationError", 3)?;
    ser.serialize_field("context", &context)?;
    ser.serialize_field("serde_error", &serde_error.to_string())?;
    ser.serialize_field("backtrace", &backtrace.to_string())?;
    ser.end()
}

fn serialize_json_deserialization_error<S>(
    context: &String,
    serde_error: &serde_json::Error,
    backtrace: &Backtrace,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("JsonDeserializationError", 4)?;
    ser.serialize_field("context", &context)?;
    ser.serialize_field("serde_error", &serde_error.to_string())?;
    ser.serialize_field("backtrace", &backtrace.to_string())?;
    ser.end()
}

impl DetailedError for CommonError {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::IoError { .. } => Cow::from("IoError"),
            Self::JsonSerializationError { .. } => Cow::from("JsonSerializationError"),
            Self::JsonDeserializationError { .. } => Cow::from("JsonDeserializationError"),
            Self::EnterpriseFeature { .. } => Cow::from("EnterpriseFeature"),
        }
    }
}

impl Display for CommonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IoError {
                context, io_error, ..
            } => {
                write!(f, "I/O error {context}: {io_error}")
            }
            Self::JsonSerializationError {
                context,
                serde_error,
                ..
            } => {
                write!(
                    f,
                    "Error during JSON serialization of {context}: {serde_error}"
                )
            }
            Self::JsonDeserializationError {
                context,
                serde_error,
                ..
            } => {
                write!(
                    f,
                    "Error during JSON deserialization of {context}: {serde_error}"
                )
            }
            Self::EnterpriseFeature { feature } => {
                write!(
                    f,
                    "Cannot use enterprise-only feature ({feature}) in Feldera community edition."
                )
            }
        }
    }
}

impl From<CommonError> for ErrorResponse {
    fn from(val: CommonError) -> Self {
        ErrorResponse::from(&val)
    }
}

impl StdError for CommonError {}

impl ResponseError for CommonError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::IoError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::JsonSerializationError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::JsonDeserializationError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::EnterpriseFeature { .. } => StatusCode::NOT_IMPLEMENTED,
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponseBuilder::new(self.status_code()).json(ErrorResponse::from_error(self))
    }
}
