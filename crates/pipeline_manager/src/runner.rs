use crate::{
    api::ManagerError,
    auth::TenantId,
    db::{storage::Storage, DBError, PipelineId, PipelineRuntimeState, PipelineStatus, ProjectDB},
};
use actix_web::{
    body::BoxBody,
    http::{Method, StatusCode},
    web::Payload,
    HttpRequest, HttpResponse, HttpResponseBuilder, ResponseError,
};
use pipeline_types::error::{DetailedError, ErrorResponse};
use serde::Serialize;
use std::{borrow::Cow, error::Error as StdError, fmt, fmt::Display, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum RunnerError {
    PipelineShutdown {
        pipeline_id: PipelineId,
    },
    HttpForwardError {
        pipeline_id: PipelineId,
        error: String,
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
    IllegalPipelineStateTransition {
        pipeline_id: PipelineId,
        error: String,
        current_status: PipelineStatus,
        desired_status: PipelineStatus,
        requested_status: Option<PipelineStatus>,
    },
    BinaryFetchError {
        pipeline_id: PipelineId,
        error: String,
    },
}

impl DetailedError for RunnerError {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::PipelineShutdown { .. } => Cow::from("PipelineShutdown"),
            Self::HttpForwardError { .. } => Cow::from("HttpForwardError"),
            Self::PortFileParseError { .. } => Cow::from("PortFileParseError"),
            Self::PipelineProvisioningTimeout { .. } => Cow::from("PipelineProvisioningTimeout"),
            Self::PipelineInitializationTimeout { .. } => {
                Cow::from("PipelineInitializationTimeout")
            }
            Self::PipelineShutdownTimeout { .. } => Cow::from("PipelineShutdownTimeout"),
            Self::PipelineStartupError { .. } => Cow::from("PipelineStartupError"),
            Self::PipelineShutdownError { .. } => Cow::from("PipelineShutdownError"),
            Self::IllegalPipelineStateTransition { .. } => {
                Cow::from("IllegalPipelineStateTransition")
            }
            Self::BinaryFetchError { .. } => Cow::from("BinaryFetchError"),
        }
    }
}

impl Display for RunnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PipelineShutdown { pipeline_id } => {
                write!(f, "Pipeline {pipeline_id} is not currently running.")
            }
            Self::HttpForwardError { pipeline_id, error } => {
                write!(
                    f,
                    "Error forwarding HTTP request to pipeline {pipeline_id}: {error}"
                )
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
            Self::IllegalPipelineStateTransition { error, .. } => {
                write!(
                    f,
                    "Action is not applicable in the current state of the pipeline: {error}"
                )
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
            Self::PipelineShutdown { .. } => StatusCode::NOT_FOUND,
            Self::HttpForwardError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PortFileParseError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineProvisioningTimeout { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineInitializationTimeout { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineShutdownTimeout { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineStartupError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineShutdownError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::IllegalPipelineStateTransition { .. } => StatusCode::BAD_REQUEST,
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
    db: Arc<Mutex<ProjectDB>>,
}

impl RunnerApi {
    /// Timeout for a HTTP request to a pipeline. This is the maximum time to
    /// wait for the issued request to attain an outcome (be it success or
    /// failure). Upon timeout, the request is failed and immediately
    /// returns.
    const PIPELINE_HTTP_REQUEST_TIMEOUT: Duration = Duration::from_millis(2_000);

    /// Create a local runner.
    pub fn new(db: Arc<Mutex<ProjectDB>>) -> Self {
        Self { db }
    }

    /// Initiate pipeline shutdown.
    ///
    /// Sets desired pipeline state to [`PipelineStatus::Paused`].
    pub(crate) async fn shutdown_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<(), ManagerError> {
        self.set_desired_status(tenant_id, pipeline_name, PipelineStatus::Shutdown)
            .await?;
        Ok(())
    }

    /// Delete the pipeline.
    ///
    /// Both the desired and the actual states of the pipeline must be equal
    /// to [`PipelineStatus::Shutdown`] or [`PipelineStatus::Failed`].
    pub(crate) async fn delete_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<(), ManagerError> {
        // Make sure that the pipeline is in a `Shutdown` state.

        // TODO: this function should run in a transaction to avoid conflicts with
        // another manager instance.
        let db = self.db.lock().await;

        let pipeline = db.get_pipeline_by_name(tenant_id, pipeline_name).await?;
        let pipeline_state = db
            .get_pipeline_runtime_state_by_id(tenant_id, pipeline.descriptor.pipeline_id)
            .await?;
        Self::validate_desired_state_request(
            pipeline.descriptor.pipeline_id,
            &pipeline_state,
            None,
        )?;

        db.delete_pipeline(tenant_id, pipeline_name).await?;

        // No need to do anything else since the pipeline was in the `Shutdown` state.
        // The pipeline tokio task will self-destruct when it polls pipeline
        // state and discovers it has been deleted.

        Ok(())
    }

    /// Set the desired state of the pipeline to [`PipelineStatus::Paused`].
    ///
    /// If the pipeline is currently in the `Shutdown` state, will validate
    /// and commit the pipeline before running it.
    pub(crate) async fn pause_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<(), ManagerError> {
        self.set_desired_status(tenant_id, pipeline_name, PipelineStatus::Paused)
            .await?;
        Ok(())
    }

    /// Set the desired state of the pipeline to [`PipelineStatus::Running`].
    ///
    /// If the pipeline is currently in the `Shutdown` state, will validate
    /// and commit the pipeline before running it.
    pub(crate) async fn start_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<(), ManagerError> {
        self.set_desired_status(tenant_id, pipeline_name, PipelineStatus::Running)
            .await?;
        Ok(())
    }

    /// Check the `request` is a valid new desired state given the current
    /// runtime state of the pipeline.  `request` value of `None` represents
    /// the request to delete the pipeline.
    fn validate_desired_state_request(
        pipeline_id: PipelineId,
        pipeline_state: &PipelineRuntimeState,
        request: Option<PipelineStatus>,
    ) -> Result<(), ManagerError> {
        match request {
            None => {
                if pipeline_state.current_status != PipelineStatus::Shutdown
                    || pipeline_state.desired_status != PipelineStatus::Shutdown
                {
                    Err(RunnerError::IllegalPipelineStateTransition {
                        pipeline_id,
                        error: "Cannot delete a running pipeline. Shutdown the pipeline first by invoking the '/shutdown' endpoint.".to_string(),
                        current_status: pipeline_state.current_status,
                        desired_status: pipeline_state.desired_status,
                        requested_status: None,
                    })?
                };
            }
            Some(new_desired_status) => {
                if new_desired_status == PipelineStatus::Paused
                    || new_desired_status == PipelineStatus::Running
                {
                    // Refuse to restart a pipeline that has not completed shutting down.
                    if pipeline_state.desired_status == PipelineStatus::Shutdown
                        && pipeline_state.current_status != PipelineStatus::Shutdown
                    {
                        Err(RunnerError::IllegalPipelineStateTransition {
                            pipeline_id,
                            error: "Cannot restart the pipeline while it is shutting down. Wait for the shutdown to complete before starting a new instance of the pipeline.".to_string(),
                            current_status: pipeline_state.current_status,
                            desired_status: pipeline_state.desired_status,
                            requested_status: Some(new_desired_status),
                        })?;
                    };

                    // Refuse to restart failed pipeline until it's in the shutdown state.
                    if pipeline_state.desired_status != PipelineStatus::Shutdown
                        && (pipeline_state.current_status == PipelineStatus::ShuttingDown
                            || pipeline_state.current_status == PipelineStatus::Failed)
                    {
                        Err(RunnerError::IllegalPipelineStateTransition {
                            pipeline_id,
                            error: "Cannot restart a failed pipeline. Clear the error state first by invoking the '/shutdown' endpoint.".to_string(),
                            current_status: pipeline_state.current_status,
                            desired_status: pipeline_state.desired_status,
                            requested_status: Some(new_desired_status),
                        })?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn set_desired_status(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
        new_desired_status: PipelineStatus,
    ) -> Result<(), ManagerError> {
        // TODO: this function should run in a transaction to avoid conflicts with
        // another manager instance.

        let db = self.db.lock().await;
        let pipeline_state = db
            .get_pipeline_runtime_state_by_name(tenant_id, pipeline_name)
            .await?;
        let pipeline_id = pipeline_state.pipeline_id;

        Self::validate_desired_state_request(
            pipeline_id,
            &pipeline_state,
            Some(new_desired_status),
        )?;

        // When starting a previously shutdown pipeline, commit its config first.
        if pipeline_state.current_status == PipelineStatus::Shutdown
            && new_desired_status != PipelineStatus::Shutdown
        {
            Self::commit_revision(&db, tenant_id, pipeline_id).await?;
        }

        db.set_pipeline_desired_status(tenant_id, pipeline_id, new_desired_status)
            .await?;
        Ok(())
    }

    /// Retrieve the last revision for a pipeline.
    ///
    /// Tries to create a new revision if this pipeline never had a revision
    /// created before.
    async fn commit_revision(
        db: &ProjectDB,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<(), ManagerError> {
        // Make sure we create a revision by updating to latest config state
        match db
            .create_pipeline_deployment(Uuid::now_v7(), tenant_id, pipeline_id)
            .await
        {
            Ok(_revision) => Ok(()),
            Err(DBError::RevisionNotChanged) => Ok(()),
            Err(e) => Err(e)?,
        }
    }

    /// Forward HTTP request to the pipeline.
    pub(crate) async fn forward_to_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
        method: Method,
        endpoint: &str,
    ) -> Result<HttpResponse, ManagerError> {
        let pipeline_state = self
            .db
            .lock()
            .await
            .get_pipeline_runtime_state_by_name(tenant_id, pipeline_name)
            .await?;
        let pipeline_id = pipeline_state.pipeline_id;

        match pipeline_state.current_status {
            PipelineStatus::Shutdown | PipelineStatus::Failed | PipelineStatus::Provisioning => {
                Err(RunnerError::PipelineShutdown { pipeline_id })?
            }
            _ => {}
        }

        Self::do_forward_to_pipeline(pipeline_id, method, endpoint, &pipeline_state.location).await
    }

    /// Forward HTTP request to pipeline.  Assumes that the pipeline is running.
    /// Takes pipeline port as an argument instead of reading it from the
    /// database.
    async fn do_forward_to_pipeline(
        pipeline_id: PipelineId,
        method: Method,
        endpoint: &str,
        location: &str,
    ) -> Result<HttpResponse, ManagerError> {
        let response = Self::pipeline_http_request(pipeline_id, method, endpoint, location).await?;
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
    ) -> Result<reqwest::Response, RunnerError> {
        let client = reqwest::Client::new();
        client
            .request(method, &format!("http://{location}/{endpoint}",))
            .timeout(Self::PIPELINE_HTTP_REQUEST_TIMEOUT)
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
        pipeline_id: PipelineId,
        endpoint: &str,
        req: HttpRequest,
        body: Payload,
        client: &awc::Client,
    ) -> Result<HttpResponse, ManagerError> {
        let pipeline_state = self
            .db
            .lock()
            .await
            .get_pipeline_runtime_state_by_id(tenant_id, pipeline_id)
            .await?;

        match pipeline_state.current_status {
            PipelineStatus::Shutdown | PipelineStatus::Failed | PipelineStatus::Provisioning => {
                Err(RunnerError::PipelineShutdown { pipeline_id })?
            }
            _ => {}
        }
        let location = pipeline_state.location;

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
