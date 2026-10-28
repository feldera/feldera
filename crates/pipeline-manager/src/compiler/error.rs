use actix_web::{HttpResponse, ResponseError, body::BoxBody, http::StatusCode};
use feldera_types::error::{DetailedError, ErrorResponse};
use serde::Serialize;
use std::{borrow::Cow, error::Error as StdError, fmt, fmt::Display};

/// The [`CompilerError`] encompasses compiler-related errors that can bubble up.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum CompilerError {
    PrecompilationError {
        error: String,
    },
    TaskFailed {
        error: String,
    },
    ClientCertificateRequired {
        method: String,
        path: String,
    },
    CallerNotAllowed {
        caller: String,
        method: String,
        path: String,
    },
}

impl DetailedError for CompilerError {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::PrecompilationError { .. } => Cow::from("PrecompilationError"),
            Self::TaskFailed { .. } => Cow::from("TaskFailed"),
            Self::ClientCertificateRequired { .. } => Cow::from("ClientCertificateRequired"),
            Self::CallerNotAllowed { .. } => Cow::from("CallerNotAllowed"),
        }
    }
}

impl Display for CompilerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PrecompilationError { error } => {
                write!(f, "Pre-compilation failed due to: {error}")
            }
            Self::TaskFailed { error } => {
                write!(f, "Compiler failed due to a task failed: {error}")
            }
            Self::ClientCertificateRequired { method, path } => {
                write!(
                    f,
                    "The compiler server requires a client certificate for {method} {path}: present a certificate that the private CA signed (control plane: --https-client-cert-path)"
                )
            }
            Self::CallerNotAllowed {
                caller,
                method,
                path,
            } => {
                write!(
                    f,
                    "{caller} may not send {method} {path} to the compiler server: a pipeline may only download its own binary and program info"
                )
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
            Self::TaskFailed { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ClientCertificateRequired { .. } => StatusCode::UNAUTHORIZED,
            Self::CallerNotAllowed { .. } => StatusCode::FORBIDDEN,
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        crate::error::json_error_response(self)
    }
}
