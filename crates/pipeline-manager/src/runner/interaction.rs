use crate::config::ApiServerConfig;
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::{ExtendedPipelineDescrMonitoring, PipelineStatus};
use crate::db::types::tenant::TenantId;
use crate::db_notifier::DbNotification;
use crate::error::ManagerError;
use crate::runner::error::RunnerError;
use actix_web::{http::Method, web::Payload, HttpRequest, HttpResponse, HttpResponseBuilder};
use awc::error::{ConnectError, SendRequestError};
use crossbeam::sync::ShardedLock;
use std::fmt::Display;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio::time::Instant;

pub(crate) struct CachedPipelineDescr {
    pipeline: ExtendedPipelineDescrMonitoring,
    instantiated: Instant,
}

impl CachedPipelineDescr {
    const CACHE_TTL: Duration = Duration::from_secs(5);

    /// Return the deployment location only if the pipeline is in the correct
    /// status to be interacted with (i.e., running or paused).
    fn deployment_location_based_on_status(&self) -> Result<String, ManagerError> {
        match self.pipeline.deployment_status {
            PipelineStatus::Running | PipelineStatus::Paused => {}
            PipelineStatus::Unavailable => Err(RunnerError::PipelineInteractionUnreachable {
                error: "deployment status is currently 'unavailable' -- wait for it to become 'running' or 'paused' again".to_string()
            })?,
            status => Err(RunnerError::PipelineInteractionNotDeployed {
                status,
                desired_status: self.pipeline.deployment_desired_status
            })?,
        };

        Ok(match &self.pipeline.deployment_location {
            None => Err(RunnerError::PipelineInteractionUnreachable {
                error: "deployment location is missing despite status being 'running' or 'paused'"
                    .to_string(),
            })?,
            Some(location) => location.clone(),
        })
    }
}

impl From<ExtendedPipelineDescrMonitoring> for CachedPipelineDescr {
    fn from(pipeline: ExtendedPipelineDescrMonitoring) -> Self {
        Self {
            pipeline,
            instantiated: Instant::now(),
        }
    }
}

/// Formats the URL to reach the pipeline.
pub fn format_pipeline_url(location: &str, endpoint: &str, query_string: &str) -> String {
    format!("http://{location}/{endpoint}?{query_string}")
}

/// Formats the error message displayed when a request to a pipeline timed out.
pub fn format_timeout_error_message<T: Display>(timeout: Duration, error: T) -> String {
    format!(
        "timeout ({}s) was reached: this means the pipeline took too long to respond -- \
         this can simply be because the request was too difficult to process in time, \
         or other reasons (e.g., deadlock): the pipeline logs might contain \
         additional information (original send request error: {error})",
        timeout.as_secs()
    )
}

/// Formats the error message displayed when a request to a pipeline experiences
/// a disconnection at the HTTP connector level.
///
/// The original error message is:
///   "Failed to connect to host: Internal error: connector has been disconnected"
///
/// ... which is replaced because it does not explain well the cause and the term
/// "connector" refers to the `awc` HTTP connector, not a Feldera connector, which
/// is confusing.
pub fn format_disconnected_error_message<T: Display>(_error: T) -> String {
    "the pipeline disconnected while it was processing this HTTP request. This could be because \
    the pipeline either (a) encountered a fatal error or panic, (b) was shutdown, or (c) \
    experienced network issues -- retrying might help in the last case. Alternatively, \
    check the pipeline logs."
        .to_string()
}

/// Helper for the API server endpoints to interact through HTTP with a pipeline or a pipeline runner.
pub struct RunnerInteraction {
    config: ApiServerConfig,
    db: Arc<Mutex<StoragePostgres>>,
    endpoint_cache: Arc<ShardedLock<HashMap<(TenantId, String), CachedPipelineDescr>>>,
}

impl RunnerInteraction {
    /// Default timeout for an HTTP request to a pipeline.
    const PIPELINE_HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

    /// Default timeout for an HTTP request to a pipeline runner.
    const RUNNER_HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

    /// Creates the interaction interface.
    /// The database is used to retrieve pipelines.
    pub fn new(config: ApiServerConfig, db: Arc<Mutex<StoragePostgres>>) -> Self {
        let endpoint_cache = Arc::new(ShardedLock::new(HashMap::new()));
        {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            tokio::spawn(crate::db_notifier::listen(db.clone(), tx));
            let endpoint_cache = endpoint_cache.clone();
            tokio::spawn(async move {
                while let Some(DbNotification::Pipeline(_op, _tenant, _pipeline_id)) =
                    rx.recv().await
                {
                    let mut cache = endpoint_cache.write().unwrap();
                    cache.clear();
                }
            });
        }

        Self {
            config,
            db,
            endpoint_cache,
        }
    }

    /// Checks that the pipeline (1) exists and retrieves it, (2) is either running or paused,
    /// and (3) has a deployment location. Returns the retrieved deployment location.
    async fn check_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<(String, bool), ManagerError> {
        let cache = self.endpoint_cache.read().unwrap();
        let entry = cache
            .get(&(tenant_id, pipeline_name.to_string()))
            .filter(|entry| entry.instantiated.elapsed() <= CachedPipelineDescr::CACHE_TTL);
        match entry {
            Some(entry) => {
                let location = entry.deployment_location_based_on_status()?;
                Ok((location, true))
            }
            None => {
                drop(cache);
                let pipeline = self
                    .db
                    .lock()
                    .await
                    .get_pipeline_for_monitoring(tenant_id, pipeline_name)
                    .await?;
                let cached_descriptor: CachedPipelineDescr = pipeline.into();
                let deployment_location = cached_descriptor.deployment_location_based_on_status();
                let mut cache = self.endpoint_cache.write().unwrap();
                cache.insert((tenant_id, pipeline_name.to_string()), cached_descriptor);
                let location = deployment_location?;
                Ok((location, false))
            }
        }
    }

    /// Makes a new HTTP request without body to the pipeline.
    /// The response is fully composed before returning including headers.
    ///
    /// This method is static as it is directly provided the pipeline
    /// identifier and location. It thus does not need to retrieve it
    /// from the database.
    pub async fn forward_http_request_to_pipeline(
        client: &awc::Client,
        location: &str,
        method: Method,
        endpoint: &str,
        query_string: &str,
        timeout: Option<Duration>,
    ) -> Result<HttpResponse, ManagerError> {
        // Perform request to the pipeline
        let url = format_pipeline_url(location, endpoint, query_string);
        let timeout = timeout.unwrap_or(Self::PIPELINE_HTTP_REQUEST_TIMEOUT);
        let mut original_response = client
            .request(method, &url)
            .timeout(timeout)
            .send()
            .await
            .map_err(|e| match e {
                SendRequestError::Timeout => RunnerError::PipelineInteractionUnreachable {
                    error: format_timeout_error_message(timeout, e),
                },
                SendRequestError::Connect(ConnectError::Disconnected) => {
                    RunnerError::PipelineInteractionUnreachable {
                        error: format_disconnected_error_message(e),
                    }
                }
                _ => RunnerError::PipelineInteractionUnreachable {
                    error: format!("unable to send request due to: {e}"),
                },
            })?;
        let status = original_response.status();

        // Build the HTTP response with the original status
        let mut response_builder = HttpResponse::build(status);

        // Add all the same headers as the original response,
        // excluding `Connection` as this is proxy, as per:
        // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Connection#Directives
        for (header_name, header_value) in original_response
            .headers()
            .iter()
            .filter(|(h, _)| *h != "connection")
        {
            response_builder.insert_header((header_name.clone(), header_value.clone()));
        }

        // Copy over the original response body
        let response_body = original_response.body().await.map_err(|e| {
            RunnerError::PipelineInteractionInvalidResponse {
                error: format!("unable to reconstruct response body due to: {e}"),
            }
        })?;
        Ok(response_builder.body(response_body))
    }

    /// Makes a new HTTP request without body to the pipeline.
    /// The response is fully composed before returning including headers.
    ///
    /// The pipeline location is retrieved from the database using the
    /// provided tenant identifier and pipeline name.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn forward_http_request_to_pipeline_by_name(
        &self,
        client: &awc::Client,
        tenant_id: TenantId,
        pipeline_name: &str,
        method: Method,
        endpoint: &str,
        query_string: &str,
        timeout: Option<Duration>,
    ) -> Result<HttpResponse, ManagerError> {
        let (location, cache_hit) = self.check_pipeline(tenant_id, pipeline_name).await?;
        let r = RunnerInteraction::forward_http_request_to_pipeline(
            client,
            &location,
            method.clone(),
            endpoint,
            query_string,
            timeout,
        )
        .await;

        match r {
            Ok(response) => Ok(response),
            Err(e) => {
                if !cache_hit {
                    Err(e)
                } else {
                    // In case of a cache hit&error, we remove the cache entry and retry the request
                    // as the cache entry might be outdated. The only time this solves a problem is
                    // when a pipeline transitions to a different state (e.g. from running
                    // to paused to running) and we have not yet processed the notification from the
                    // database that evicts the cache (this scenario is highly unlikely not just because
                    // we've probably processed the notification, but also because we have to do 3 pipeline
                    // transitions within the cache ttl of 5 secs).
                    let mut cache = self.endpoint_cache.write().unwrap();
                    cache.remove(&(tenant_id, pipeline_name.to_string()));
                    drop(cache);
                    Box::pin(self.forward_http_request_to_pipeline_by_name(
                        client,
                        tenant_id,
                        pipeline_name,
                        method,
                        endpoint,
                        query_string,
                        timeout,
                    ))
                    .await
                }
            }
        }
    }

    /// Forwards the provided HTTP request to the pipeline, for which the
    /// request body can be streaming, and the response body is streaming.
    /// The response has all headers.
    ///
    /// The pipeline location is retrieved from the database using the
    /// provided tenant identifier and pipeline name.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn forward_streaming_http_request_to_pipeline_by_name(
        &self,
        client: &awc::Client,
        tenant_id: TenantId,
        pipeline_name: &str,
        endpoint: &str,
        request: HttpRequest,
        body: Payload,
        timeout: Option<Duration>, // If no timeout is specified, a default timeout is used
    ) -> Result<HttpResponse, ManagerError> {
        let (location, _cache_hit) = self.check_pipeline(tenant_id, pipeline_name).await?;

        // Build new request to pipeline
        let url = format_pipeline_url(&location, endpoint, request.query_string());
        let timeout = timeout.unwrap_or(Self::PIPELINE_HTTP_REQUEST_TIMEOUT);
        let mut new_request = client
            .request(request.method().clone(), &url)
            .timeout(timeout);

        // Add headers of the original request
        for header in request
            .headers()
            .into_iter()
            .filter(|(h, _)| *h != "connection")
        {
            new_request = new_request.append_header(header);
        }

        // Perform request to the pipeline
        let response = new_request.send_stream(body).await.map_err(|e| match e {
            SendRequestError::Timeout => RunnerError::PipelineInteractionUnreachable {
                error: format_timeout_error_message(timeout, e),
            },
            SendRequestError::Connect(ConnectError::Disconnected) => {
                RunnerError::PipelineInteractionUnreachable {
                    error: format_disconnected_error_message(e),
                }
            }
            _ => RunnerError::PipelineInteractionUnreachable {
                error: format!("unable to send request due to: {e}"),
            },
        })?;

        // Build the new HTTP response with the same status, headers and streaming body
        let mut builder = HttpResponseBuilder::new(response.status());
        for header in response.headers().into_iter() {
            builder.append_header(header);
        }
        Ok(builder.streaming(response))
    }

    /// Retrieves the streaming logs of the pipeline through the runner.
    ///
    /// The pipeline identifier is retrieved from the database using the
    /// provided tenant identifier and pipeline name.
    pub(crate) async fn http_streaming_logs_from_pipeline_by_name(
        &self,
        client: &awc::Client,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<HttpResponse, ManagerError> {
        // Retrieve pipeline
        let pipeline = self
            .db
            .lock()
            .await
            .get_pipeline_for_monitoring(tenant_id, pipeline_name)
            .await?;

        // Only in Shutdown status do we not attempt to reach out to the pipeline runner
        if pipeline.deployment_status == PipelineStatus::Shutdown {
            return Err(ManagerError::from(RunnerError::RunnerInteractionShutdown));
        }

        // Build request to the runner
        let url = format!(
            "http://{}/logs/{}",
            self.config.runner_hostname_port, pipeline.id
        );

        // Perform request to the runner
        let response = client
            .request(Method::GET, &url)
            .timeout(Self::RUNNER_HTTP_REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(|e| {
                match e {
                    SendRequestError::Timeout => {
                        RunnerError::RunnerInteractionUnreachable {
                            error: format!(
                                "timeout ({}s) was reached: this means the runner took too long to respond -- \
                                 the runner logs might contain additional information (original send request error: {e})",
                                Self::RUNNER_HTTP_REQUEST_TIMEOUT.as_secs()
                            )
                        }
                    }
                    _ => {
                        RunnerError::RunnerInteractionUnreachable {
                            error: format!("unable to send request due to: {e}"),
                        }
                    }
                }
            })?;

        // Build the HTTP response with the same status, headers and streaming body
        let mut builder = HttpResponseBuilder::new(response.status());
        for header in response.headers().into_iter() {
            builder.append_header(header);
        }
        Ok(builder.streaming(response))
    }
}
