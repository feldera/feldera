//! This module implements [`enum ManagerError`], the top-level error
//! type returned by HTTP endpoints across the manager components.
//!
//! At a high level, the error hierarchy looks like this:
//!
//! ```text
//! ManagerError (this module)
//! ├─ common_error::CommonError (errors returned by the `common_error` module)
//! ├─ db::DBError (errors returned by the `db` module)
//! ├─ api::ApiError (errors returned by the `api` module)
//! ├─ compiler::CompilerError (errors returned by the `compiler` module)
//! ├─ runner::RunnerError (errors returned by the `runner` module)
//! ├─ demo::DemoError (errors returned by the `demo` module)
//! ```
//!
//! We implement the `actix-web` `ResponseError` trait for all error types,
//! which allows them to be returned as error responses by HTTP endpoints.
//! Our `ResponseError` implementation generates an HTTP response whose body
//! is a JSON serialization of the `feldera_types::ErrorResponse` type from the
//! `feldera_types` crate, i.e., errors returned by the API server and
//! by individual pipelines have the same format.

use crate::api::demo::DemoError;
use crate::api::error::ApiError;
use crate::common_error::CommonError;
use crate::compiler::error::CompilerError;
use crate::db::error::DBError;
use crate::runner::error::RunnerError;
use actix_web::{
    HttpResponse, HttpResponseBuilder, ResponseError, body::BoxBody, http::StatusCode, http::header,
};
use feldera_types::error::{DetailedError, ErrorResponse};
use openssl::error::ErrorStack;
use serde::Serialize;
use std::{
    borrow::Cow,
    error::Error as StdError,
    fmt::{Display, Error as FmtError, Formatter},
};

/// Pipeline manager errors.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ManagerError {
    CommonError {
        #[serde(flatten)]
        common_error: CommonError,
    },
    DBError {
        #[serde(flatten)]
        db_error: DBError,
    },
    ApiError {
        #[serde(flatten)]
        api_error: ApiError,
    },
    CompilerError {
        #[serde(flatten)]
        compiler_error: CompilerError,
    },
    RunnerError {
        #[serde(flatten)]
        runner_error: RunnerError,
    },
    DemoError {
        #[serde(flatten)]
        demo_error: DemoError,
    },
}

impl StdError for ManagerError {}

impl From<CommonError> for ManagerError {
    fn from(common_error: CommonError) -> Self {
        Self::CommonError { common_error }
    }
}

impl From<DBError> for ManagerError {
    fn from(db_error: DBError) -> Self {
        Self::DBError { db_error }
    }
}

impl From<CompilerError> for ManagerError {
    fn from(compiler_error: CompilerError) -> Self {
        Self::CompilerError { compiler_error }
    }
}

impl From<ApiError> for ManagerError {
    fn from(api_error: ApiError) -> Self {
        Self::ApiError { api_error }
    }
}

impl From<RunnerError> for ManagerError {
    fn from(runner_error: RunnerError) -> Self {
        Self::RunnerError { runner_error }
    }
}

impl From<ErrorStack> for ManagerError {
    fn from(value: ErrorStack) -> Self {
        Self::RunnerError {
            runner_error: RunnerError::OpenSSL {
                errors: value.to_string(),
            },
        }
    }
}

impl Display for ManagerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Self::CommonError { common_error } => common_error.fmt(f),
            Self::DBError { db_error } => db_error.fmt(f),
            Self::ApiError { api_error } => api_error.fmt(f),
            Self::CompilerError { compiler_error } => compiler_error.fmt(f),
            Self::RunnerError { runner_error } => runner_error.fmt(f),
            Self::DemoError { demo_error } => {
                write!(f, "Demo configuration error: '{demo_error}'")
            }
        }
    }
}

/// Seconds a client should wait before retrying a 503. Matches the runner's
/// fastest pipeline-status probe interval and its pipeline-descriptor cache
/// TTL: the soonest an unavailable pipeline can be observed healthy again.
pub(crate) const SERVICE_UNAVAILABLE_RETRY_AFTER_SECONDS: u32 = 5;

/// Build the JSON error response. A 503 carries `Retry-After` so clients
/// back off for a server-chosen interval instead of guessing: 503s signal a
/// transient condition (pipeline pod rescheduling, runner restart) that
/// resolves within seconds.
pub(crate) fn json_error_response<E>(error: &E) -> HttpResponse<BoxBody>
where
    E: DetailedError,
{
    let status = error.status_code();
    let mut builder = HttpResponseBuilder::new(status);
    if status == StatusCode::SERVICE_UNAVAILABLE {
        builder.insert_header((
            header::RETRY_AFTER,
            SERVICE_UNAVAILABLE_RETRY_AFTER_SECONDS.to_string(),
        ));
    }
    builder.json(ErrorResponse::from_error(error))
}

impl ResponseError for ManagerError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::CommonError { common_error } => common_error.status_code(),
            Self::DBError { db_error } => db_error.status_code(),
            Self::ApiError { api_error } => api_error.status_code(),
            Self::CompilerError { compiler_error } => compiler_error.status_code(),
            Self::RunnerError { runner_error } => runner_error.status_code(),
            Self::DemoError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        json_error_response(self)
    }
}

impl DetailedError for ManagerError {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::CommonError { common_error } => common_error.error_code(),
            Self::DBError { db_error } => db_error.error_code(),
            Self::ApiError { api_error } => api_error.error_code(),
            Self::CompilerError { compiler_error } => compiler_error.error_code(),
            Self::RunnerError { runner_error } => runner_error.error_code(),
            Self::DemoError { .. } => Cow::from("DemoError"),
        }
    }
}

// helper method to get nested source error
pub(crate) fn source_error(mut err: &dyn StdError) -> &dyn StdError {
    while let Some(src) = err.source() {
        err = src;
    }
    err
}

impl From<ManagerError> for ErrorResponse {
    fn from(val: ManagerError) -> Self {
        ErrorResponse::from_error_nolog(&val)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::runner::error::RunnerError;

    #[test]
    fn service_unavailable_carries_retry_after() {
        let err = ManagerError::from(RunnerError::PipelineUnavailable {
            pipeline_name: "p".to_string(),
        });
        let resp = err.error_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            resp.headers().get(header::RETRY_AFTER).unwrap(),
            &SERVICE_UNAVAILABLE_RETRY_AFTER_SECONDS.to_string()
        );
    }

    #[test]
    fn other_statuses_carry_no_retry_after() {
        let err = ManagerError::from(RunnerError::AutomatonMissingProgramInfo);
        let resp = err.error_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(resp.headers().get(header::RETRY_AFTER).is_none());
    }
}
