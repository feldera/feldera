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
    body::BoxBody, http::StatusCode, HttpResponse, HttpResponseBuilder, ResponseError,
};
use feldera_types::error::{DetailedError, ErrorResponse};
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
        HttpResponseBuilder::new(self.status_code()).json(ErrorResponse::from_error(self))
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
