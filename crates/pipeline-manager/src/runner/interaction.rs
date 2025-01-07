use crate::config::ApiServerConfig;
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::{ExtendedPipelineDescrMonitoring, PipelineId, PipelineStatus};
use crate::db::types::tenant::TenantId;
use crate::db_notifier::DbNotification;
use crate::error::ManagerError;
use crate::runner::error::RunnerError;
use actix_web::{http::Method, web::Payload, HttpRequest, HttpResponse, HttpResponseBuilder};
use crossbeam::sync::ShardedLock;
use reqwest::StatusCode;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio::time::Instant;

pub(crate) struct CachedPipelineDescr {
    pipeline: ExtendedPipelineDescrMonitoring,
    instantiated: Instant,
}

impl CachedPipelineDescr {
    const CACHE_TTL: Duration = Duration::from_secs(5);

    fn deployment_status(&self) -> Result<(ExtendedPipelineDescrMonitoring, String), ManagerError> {
        match self.pipeline.deployment_status {
            PipelineStatus::Running | PipelineStatus::Paused => {}
            _ => Err(RunnerError::PipelineNotRunningOrPaused {
                pipeline_id: self.pipeline.id,
                pipeline_name: self.pipeline.name.clone(),
            })?,
        };

        Ok((
            self.pipeline.clone(),
            match &self.pipeline.deployment_location {
                None => Err(RunnerError::PipelineMissingDeploymentLocation {
                    pipeline_id: self.pipeline.id,
                    pipeline_name: self.pipeline.name.clone(),
                })?,
                Some(location) => location.clone(),
            },
        ))
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

/// Interface to interact through HTTP with the runner itself or the pipelines that it spawns.
pub struct RunnerInteraction {
    config: ApiServerConfig,
    db: Arc<Mutex<StoragePostgres>>,
    endpoint_cache: Arc<ShardedLock<HashMap<(TenantId, String), CachedPipelineDescr>>>,
}

impl RunnerInteraction {
    /// Default timeout for an HTTP request to a pipeline. This is the maximum time to
    /// wait for the issued request to attain an outcome (be it success or failure).
    /// Upon timeout, the request is failed and immediately returns.
    const PIPELINE_HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

    /// Default timeout for a HTTP request to the runner.
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
    ) -> Result<(ExtendedPipelineDescrMonitoring, String, bool), ManagerError> {
        let cache = self.endpoint_cache.read().unwrap();
        let entry = cache
            .get(&(tenant_id, pipeline_name.to_string()))
            .filter(|entry| entry.instantiated.elapsed() <= CachedPipelineDescr::CACHE_TTL);
        match entry {
            Some(entry) => {
                let (desc, location) = entry.deployment_status()?;
                Ok((desc, location, true))
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
                let deployment_status = cached_descriptor.deployment_status();
                let mut cache = self.endpoint_cache.write().unwrap();
                cache.insert((tenant_id, pipeline_name.to_string()), cached_descriptor);
                let (desc, location) = deployment_status?;
                Ok((desc, location, false))
            }
        }
    }

    /// Formats the URL to reach the pipeline.
    fn format_pipeline_url(location: &str, endpoint: &str, query_string: &str) -> String {
        format!("http://{location}/{endpoint}?{query_string}")
    }

    /// Makes a new HTTP request without body to the pipeline.
    /// The response is fully composed before returning.
    ///
    /// This method is static as it is directly provided the pipeline
    /// identifier and location. It thus does not need to retrieve it
    /// from the database.
    ///
    /// The response is 2-tuple of (requested URL, response).
    pub async fn http_request_to_pipeline(
        pipeline_id: PipelineId,
        pipeline_name: Option<String>, // Name is only used for improved error reporting; provide `None` if not known
        location: &str,
        method: Method,
        endpoint: &str,
        query_string: &str,
        timeout: Option<Duration>, // If timeout is not specified, a default timeout is used
    ) -> Result<(String, reqwest::Response), ManagerError> {
        let client = reqwest::Client::new();
        let url = RunnerInteraction::format_pipeline_url(location, endpoint, query_string);
        let response = client
            .request(method, &url)
            .timeout(timeout.unwrap_or(Self::PIPELINE_HTTP_REQUEST_TIMEOUT))
            .send()
            .await
            .map_err(|e| RunnerError::PipelineEndpointSendError {
                pipeline_id,
                pipeline_name: pipeline_name.clone(),
                url: url.to_string(),
                error: e.to_string(),
            })?;
        Ok((url, response))
    }

    /// Makes a new HTTP request without body to the pipeline.
    /// The response is fully composed before returning.
    ///
    /// This method is static as it is directly provided the pipeline
    /// identifier and location. It thus does not need to retrieve it
    /// from the database.
    ///
    /// The response is 2-tuple of (status code, JSON response body).
    pub(crate) async fn http_request_to_pipeline_json(
        pipeline_id: PipelineId,
        pipeline_name: Option<String>, // Name is only used for improved error reporting; provide `None` if not known
        location: &str,
        method: Method,
        endpoint: &str,
        query_string: &str,
        timeout: Option<Duration>, // If no timeout is specified, a default timeout is used
    ) -> Result<(StatusCode, serde_json::Value), ManagerError> {
        let (url, response) = RunnerInteraction::http_request_to_pipeline(
            pipeline_id,
            pipeline_name,
            location,
            method,
            endpoint,
            query_string,
            timeout,
        )
        .await?;
        let status = response.status();
        let value = response.json::<serde_json::Value>().await.map_err(|e| {
            RunnerError::PipelineEndpointResponseJsonParseError {
                pipeline_id,
                pipeline_name: None,
                url,
                error: e.to_string(),
            }
        })?;
        Ok((status, value))
    }

    /// Makes a new HTTP request without body to the pipeline.
    /// The response is fully composed before returning including headers.
    ///
    /// This method is static as it is directly provided the pipeline
    /// identifier and location. It thus does not need to retrieve it
    /// from the database.
    ///
    /// The response is 2-tuple of (requested URL, response).
    pub async fn forward_http_request_to_pipeline(
        pipeline_id: PipelineId,
        pipeline_name: Option<String>, // Name is only used for improved error reporting; provide `None` if not known
        location: &str,
        method: Method,
        endpoint: &str,
        query_string: &str,
        timeout: Option<Duration>, // If timeout is not specified, a default timeout is used
    ) -> Result<(String, HttpResponse), ManagerError> {
        // Perform request to the pipeline
        let (url, original_response) = Self::http_request_to_pipeline(
            pipeline_id,
            pipeline_name.clone(),
            location,
            method,
            endpoint,
            query_string,
            timeout,
        )
        .await?;
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
        let response_body = original_response.bytes().await.map_err(|e| {
            RunnerError::PipelineEndpointResponseBodyError {
                pipeline_id,
                pipeline_name: pipeline_name.clone(),
                url: url.to_string(),
                error: e.to_string(),
            }
        })?;
        Ok((url.to_string(), response_builder.body(response_body)))
    }

    /// Makes a new HTTP request without body to the pipeline,
    /// which is found via the tenant identifier and pipeline name.
    /// The response is fully composed before returning including headers.
    /// This function is intended to be called by the user endpoints, and
    /// has a different more informative error response when the HTTP request fails.
    pub(crate) async fn forward_http_request_to_pipeline_by_name(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
        method: Method,
        endpoint: &str,
        query_string: &str,
        timeout: Option<Duration>, // If no timeout is specified, a default timeout is used
    ) -> Result<HttpResponse, ManagerError> {
        let (pipeline, location, cache_hit) = self.check_pipeline(tenant_id, pipeline_name).await?;
        let r = RunnerInteraction::forward_http_request_to_pipeline(
            pipeline.id,
            Some(pipeline_name.to_string()),
            &location,
            method.clone(),
            endpoint,
            query_string,
            timeout,
        )
        .await
        .map(|(_url, response)| response);

        match r {
            Ok(response) => Ok(response),
            Err(e) => {
                if !cache_hit {
                    Err(ManagerError::from(RunnerError::PipelineUnreachable {
                        original_error: e.to_string(),
                    }))
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
                    Box::pin(self.forward_http_request_to_pipeline_by_name(
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

    /// Forwards HTTP request to the pipeline, with both the request
    /// and response body being streaming. The pipeline is found via
    /// the tenant identifier and pipeline name. The response with
    /// headers is returned.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn forward_streaming_http_request_to_pipeline_by_name(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
        endpoint: &str,
        request: HttpRequest,
        body: Payload,
        client: &awc::Client,
        timeout: Option<Duration>, // If no timeout is specified, a default timeout is used
    ) -> Result<HttpResponse, ManagerError> {
        let (pipeline, location, _cache_hit) =
            self.check_pipeline(tenant_id, pipeline_name).await?;

        // Build new request to pipeline
        let url =
            RunnerInteraction::format_pipeline_url(&location, endpoint, request.query_string());
        let mut new_request = client
            .request(request.method().clone(), &url)
            .timeout(timeout.unwrap_or(Self::PIPELINE_HTTP_REQUEST_TIMEOUT));

        // Add headers of the original request
        for header in request
            .headers()
            .into_iter()
            .filter(|(h, _)| *h != "connection")
        {
            new_request = new_request.append_header(header);
        }

        // Perform request to the pipeline
        let response = new_request.send_stream(body).await.map_err(|e| {
            RunnerError::PipelineEndpointSendError {
                pipeline_id: pipeline.id,
                pipeline_name: Some(pipeline.name.clone()),
                url: url.to_string(),
                error: e.to_string(),
            }
        })?;

        // Build the new HTTP response with the same status, headers and streaming body
        let mut builder = HttpResponseBuilder::new(response.status());
        for header in response.headers().into_iter() {
            builder.append_header(header);
        }
        Ok(builder.streaming(response))
    }

    /// Retrieves the streaming logs of the pipeline through the runner.
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

        // Build request to the runner
        let url = format!(
            "http://{}/logs/{}",
            self.config.runner_hostname_port, pipeline.id
        );
        let request = client
            .request(Method::GET, &url)
            .timeout(Self::RUNNER_HTTP_REQUEST_TIMEOUT);

        // Perform request to the runner
        let response = request
            .send()
            .await
            .map_err(|e| RunnerError::RunnerEndpointSendError {
                url: url.to_string(),
                error: e.to_string(),
            })
            .map_err(|e| {
                ManagerError::from(RunnerError::RunnerUnreachable {
                    original_error: e.to_string(),
                })
            })?;

        // Build the HTTP response with the same status, headers and streaming body
        let mut builder = HttpResponseBuilder::new(response.status());
        for header in response.headers().into_iter() {
            builder.append_header(header);
        }
        Ok(builder.streaming(response))
    }
}
