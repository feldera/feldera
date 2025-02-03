use actix_web::{
    body::BoxBody, http::StatusCode, HttpResponse, HttpResponseBuilder, ResponseError,
};
use feldera_types::error::{DetailedError, ErrorResponse};
use serde::Serialize;
use std::{borrow::Cow, error::Error as StdError, fmt, fmt::Display};

/// The [`CompilerError`] encompasses compiler-related errors that can bubble up.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum CompilerError {
    PrecompilationError { error: String },
}

impl DetailedError for CompilerError {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::PrecompilationError { .. } => Cow::from("PrecompilationError"),
        }
    }
}

impl Display for CompilerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PrecompilationError { error } => {
                write!(f, "Pre-compilation failed due to: {error}")
            }
        }
    }
}

impl From<CompilerError> for ErrorResponse {
    fn from(val: CompilerError) -> Self {
        ErrorResponse::from(&val)
    }
}

impl StdError for CompilerError {}

impl ResponseError for CompilerError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::PrecompilationError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponseBuilder::new(self.status_code()).json(ErrorResponse::from_error(self))
    }
}
