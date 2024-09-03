use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::{PipelineId, PipelineStatus};
use crate::db::types::program::ConnectorGenerationError;
use crate::db::types::tenant::TenantId;
use crate::error::ManagerError;
use actix_web::{
    body::BoxBody,
    http::{Method, StatusCode},
    web::Payload,
    HttpRequest, HttpResponse, HttpResponseBuilder, ResponseError,
};
use feldera_types::error::{DetailedError, ErrorResponse};
use serde::Serialize;
use std::{borrow::Cow, error::Error as StdError, fmt, fmt::Display, sync::Arc, time::Duration};
use tokio::sync::Mutex;

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum RunnerError {
    // Endpoints
    PipelineNotRunningOrPaused {
        pipeline_id: PipelineId,
    },
    HttpForwardError {
        pipeline_id: PipelineId,
        error: String,
    },
    // Runner internal
    PipelineConfigurationGenerationFailed {
        error: ConnectorGenerationError,
    },
    PortFileParseError {
        pipeline_id: PipelineId,
        error: String,
    },
    PipelineProvisioningTimeout {
        pipeline_id: PipelineId,
        timeout: Duration,
    },
    PipelineInitializationTimeout {
        pipeline_id: PipelineId,
        timeout: Duration,
    },
    PipelineShutdownTimeout {
        pipeline_id: PipelineId,
        timeout: Duration,
    },
    PipelineStartupError {
        pipeline_id: PipelineId,
        // TODO: This should be IOError, so we can serialize the error code
        // similar to `DBSPError::IO`.
        error: String,
    },
    PipelineShutdownError {
        pipeline_id: PipelineId,
        // TODO: This should be IOError, so we can serialize the error code
        // similar to `DBSPError::IO`.
        error: String,
    },
    BinaryFetchError {
        pipeline_id: PipelineId,
        error: String,
    },
}

impl DetailedError for RunnerError {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::PipelineNotRunningOrPaused { .. } => Cow::from("PipelineNotRunningOrPaused"),
            Self::HttpForwardError { .. } => Cow::from("HttpForwardError"),
            Self::PipelineConfigurationGenerationFailed { .. } => {
                Cow::from("PipelineConfigurationGenerationFailed")
            }
            Self::PortFileParseError { .. } => Cow::from("PortFileParseError"),
            Self::PipelineProvisioningTimeout { .. } => Cow::from("PipelineProvisioningTimeout"),
            Self::PipelineInitializationTimeout { .. } => {
                Cow::from("PipelineInitializationTimeout")
            }
            Self::PipelineShutdownTimeout { .. } => Cow::from("PipelineShutdownTimeout"),
            Self::PipelineStartupError { .. } => Cow::from("PipelineStartupError"),
            Self::PipelineShutdownError { .. } => Cow::from("PipelineShutdownError"),
            Self::BinaryFetchError { .. } => Cow::from("BinaryFetchError"),
        }
    }
}

impl Display for RunnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PipelineNotRunningOrPaused { pipeline_id } => {
                write!(
                    f,
                    "Pipeline {pipeline_id} is not currently running or paused."
                )
            }
            Self::HttpForwardError { pipeline_id, error } => {
                write!(
                    f,
                    "Error forwarding HTTP request to pipeline {pipeline_id}: {error}"
                )
            }
            Self::PipelineConfigurationGenerationFailed { error } => {
                write!(f, "Error generating pipeline configuration: {error}")
            }
            Self::PipelineProvisioningTimeout {
                pipeline_id,
                timeout,
            } => {
                write!(
                    f,
                    "Waiting for pipeline {pipeline_id} to start timed out after {timeout:?}"
                )
            }
            Self::PipelineInitializationTimeout {
                pipeline_id,
                timeout,
            } => {
                write!(
                    f,
                    "Waiting for pipeline {pipeline_id} initialization timed out after {timeout:?}"
                )
            }
            Self::PipelineShutdownTimeout {
                pipeline_id,
                timeout,
            } => {
                write!(
                    f,
                    "Waiting for pipeline {pipeline_id} to shutdown timed out after {timeout:?}"
                )
            }
            Self::PortFileParseError { pipeline_id, error } => {
                write!(
                    f,
                    "Could not parse port for pipeline {pipeline_id} from port file: {error}"
                )
            }
            Self::PipelineStartupError { pipeline_id, error } => {
                write!(f, "Failed to start pipeline {pipeline_id}: {error}")
            }
            Self::PipelineShutdownError { pipeline_id, error } => {
                write!(f, "Failed to shutdown pipeline '{pipeline_id}': '{error}'")
            }
            Self::BinaryFetchError { pipeline_id, error } => {
                write!(
                    f,
                    "Failed to fetch binary executable for running pipeline {pipeline_id}: {error}"
                )
            }
        }
    }
}

impl From<RunnerError> for ErrorResponse {
    fn from(val: RunnerError) -> Self {
        ErrorResponse::from(&val)
    }
}

impl StdError for RunnerError {}

impl ResponseError for RunnerError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::PipelineNotRunningOrPaused { .. } => StatusCode::BAD_REQUEST,
            Self::HttpForwardError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineConfigurationGenerationFailed { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PortFileParseError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineProvisioningTimeout { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineInitializationTimeout { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineShutdownTimeout { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineStartupError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineShutdownError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::BinaryFetchError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponseBuilder::new(self.status_code()).json(ErrorResponse::from_error(self))
    }
}

/// Interface to express pipeline desired states to runners and also
/// connect to streams
pub struct RunnerApi {
    db: Arc<Mutex<StoragePostgres>>,
}

impl RunnerApi {
    /// Timeout for a HTTP request to a pipeline. This is the maximum time to
    /// wait for the issued request to attain an outcome (be it success or
    /// failure). Upon timeout, the request is failed and immediately
    /// returns.
    const PIPELINE_HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

    /// Create a local runner.
    pub fn new(db: Arc<Mutex<StoragePostgres>>) -> Self {
        Self { db }
    }

    /// Forward HTTP request to the pipeline.
    pub(crate) async fn forward_to_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
        method: Method,
        endpoint: &str,
        query_string: &str,
        timeout: Option<Duration>,
    ) -> Result<HttpResponse, ManagerError> {
        let pipeline = self
            .db
            .lock()
            .await
            .get_pipeline(tenant_id, pipeline_name)
            .await?;
        let pipeline_id = pipeline.id;

        match pipeline.deployment_status {
            PipelineStatus::Running | PipelineStatus::Paused => {}
            _ => Err(RunnerError::PipelineNotRunningOrPaused { pipeline_id })?,
        }

        Self::do_forward_to_pipeline(
            pipeline_id,
            method,
            endpoint,
            &pipeline.deployment_location.unwrap(),
            query_string,
            timeout,
        )
        .await // TODO: unwrap
    }

    /// Forward HTTP request to pipeline.  Assumes that the pipeline is running.
    /// Takes pipeline port as an argument instead of reading it from the
    /// database.
    async fn do_forward_to_pipeline(
        pipeline_id: PipelineId,
        method: Method,
        endpoint: &str,
        location: &str,
        query_string: &str,
        timeout: Option<Duration>,
    ) -> Result<HttpResponse, ManagerError> {
        let response = Self::pipeline_http_request(
            pipeline_id,
            method,
            endpoint,
            location,
            query_string,
            timeout,
        )
        .await?;
        let status = response.status();

        let mut response_builder = HttpResponse::build(status);
        // Remove `Connection` as per
        // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Connection#Directives
        for (header_name, header_value) in response
            .headers()
            .iter()
            .filter(|(h, _)| *h != "connection")
        {
            response_builder.insert_header((header_name.clone(), header_value.clone()));
        }

        let response_body = response
            .bytes()
            .await
            .map_err(|e| RunnerError::HttpForwardError {
                pipeline_id,
                error: e.to_string(),
            })?;

        Ok(response_builder.body(response_body))
    }

    /// Send HTTP request to pipeline.
    pub async fn pipeline_http_request(
        pipeline_id: PipelineId,
        method: Method,
        endpoint: &str,
        location: &str,
        query_string: &str,
        timeout: Option<Duration>,
    ) -> Result<reqwest::Response, RunnerError> {
        let client = reqwest::Client::new();
        client
            .request(
                method,
                &format!("http://{location}/{endpoint}?{}", query_string),
            )
            .timeout(timeout.unwrap_or(Self::PIPELINE_HTTP_REQUEST_TIMEOUT))
            .send()
            .await
            .map_err(|e| RunnerError::HttpForwardError {
                pipeline_id,
                error: e.to_string(),
            })
    }

    pub(crate) async fn forward_to_pipeline_as_stream(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
        endpoint: &str,
        req: HttpRequest,
        body: Payload,
        client: &awc::Client,
    ) -> Result<HttpResponse, ManagerError> {
        let pipeline = self
            .db
            .lock()
            .await
            .get_pipeline(tenant_id, pipeline_name)
            .await?;
        let pipeline_id = pipeline.id;

        match pipeline.deployment_status {
            PipelineStatus::Running | PipelineStatus::Paused => {}
            _ => Err(RunnerError::PipelineNotRunningOrPaused { pipeline_id })?,
        }
        let location = pipeline.deployment_location.unwrap(); // TODO: unwrap

        // TODO: it might be better to have ?name={}, otherwise we have to
        // restrict name format
        let url = format!("http://{location}/{endpoint}?{}", req.query_string());

        let mut request = client.request(req.method().clone(), url);

        for header in req
            .headers()
            .into_iter()
            .filter(|(h, _)| *h != "connection")
        {
            request = request.append_header(header);
        }

        let response =
            request
                .send_stream(body)
                .await
                .map_err(|e| RunnerError::HttpForwardError {
                    pipeline_id,
                    error: e.to_string(),
                })?;

        let mut builder = HttpResponseBuilder::new(response.status());
        for header in response.headers().into_iter() {
            builder.append_header(header);
        }
        Ok(builder.streaming(response))
    }
}
