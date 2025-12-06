use crate::api::error::ApiError;
use crate::config::CommonConfig;
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::ExtendedPipelineDescrMonitoring;
use crate::db::types::tenant::TenantId;
use crate::error::ManagerError;
use crate::runner::error::RunnerError;
use actix_web::{http::Method, web::Payload, HttpRequest, HttpResponse, HttpResponseBuilder};
use actix_ws::{CloseCode, CloseReason};
use awc::error::{ConnectError, SendRequestError};
use awc::{ClientRequest, ClientResponse};
use crossbeam::sync::ShardedLock;
use feldera_observability::AwcRequestTracingExt;
use feldera_types::query::MAX_WS_FRAME_SIZE;
use std::fmt::Display;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::{error, info};

use crate::db::listen_table::PIPELINE_NOTIFY_CHANNEL_CAPACITY;
use crate::db::types::resources_status::ResourcesStatus;
use actix_http::encoding::Decoder;
use feldera_types::runtime_status::RuntimeStatus;

/// Max non-streaming HTTP response body returned by the pipeline.
/// The awc default is 2MiB, which is not enough to, for example, retrieve
/// a large circuit profile.
const RESPONSE_SIZE_LIMIT: usize = 20 * 1024 * 1024;

pub(crate) struct CachedPipelineDescr {
    pipeline: ExtendedPipelineDescrMonitoring,
    instantiated: Instant,
}

impl CachedPipelineDescr {
    const CACHE_TTL: Duration = Duration::from_secs(5);

    /// Return the deployment location only if the pipeline is in the correct
    /// status to be interacted with (i.e., running or paused).
    fn deployment_location_based_on_status(&self) -> Result<String, ManagerError> {
        // Interaction with the pipeline is only useful to be attempted if the pipeline has its
        // resources provisioned, and the latest runtime status check did not return indicate even
        // the runner can't interact with it.
        if self.pipeline.deployment_resources_status == ResourcesStatus::Provisioned {
            if self.pipeline.deployment_runtime_status == Some(RuntimeStatus::Unavailable) {
                return Err(ManagerError::from(RunnerError::PipelineUnavailable {
                    pipeline_name: self.pipeline.name.clone(),
                }));
            }
        } else {
            return Err(ManagerError::from(
                RunnerError::PipelineInteractionNotDeployed {
                    pipeline_name: self.pipeline.name.clone(),
                    status: self.pipeline.deployment_resources_status,
                    desired_status: self.pipeline.deployment_resources_desired_status,
                },
            ));
        }

        Ok(match &self.pipeline.deployment_location {
            None => Err(RunnerError::PipelineMissingDeploymentLocation {
                pipeline_name: self.pipeline.name.clone(),
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
pub fn format_pipeline_url(
    protocol: &str,
    location: &str,
    endpoint: &str,
    query_string: &str,
) -> String {
    format!("{protocol}://{location}/{endpoint}?{query_string}")
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

/// Format the error message displayed when a request to the runner timed out.
pub fn format_runner_timeout_error_message<T: Display>(timeout: Duration, error: T) -> String {
    format!(
        "timeout ({}s) was reached: this means the runner took too long to respond -- \
         the runner logs might contain additional information (original send request error: {error})",
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
    the pipeline either (a) encountered a fatal error or panic, (b) was stopped, or (c) \
    experienced network issues -- retrying might help in the last case. Alternatively, \
    check the pipeline logs."
        .to_string()
}

/// Helper for the API server endpoints to interact through HTTP with a pipeline or a pipeline runner.
pub struct RunnerInteraction {
    common_config: CommonConfig,
    db: Arc<Mutex<StoragePostgres>>,
    endpoint_cache: Arc<ShardedLock<HashMap<(TenantId, String), CachedPipelineDescr>>>,
}

impl RunnerInteraction {
    /// Default timeout for an HTTP request to a pipeline.
    pub const PIPELINE_HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

    /// Default timeout for an HTTP request to a pipeline runner.
    pub const RUNNER_HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

    /// Creates the interaction interface.
    /// The database is used to retrieve pipelines.
    pub fn new(common_config: CommonConfig, db: Arc<Mutex<StoragePostgres>>) -> Self {
        let endpoint_cache = Arc::new(ShardedLock::new(HashMap::new()));
        {
            let (tx, mut rx) = tokio::sync::mpsc::channel(PIPELINE_NOTIFY_CHANNEL_CAPACITY);
            tokio::spawn(crate::db::listen_table::listen_table(db.clone(), tx));
            let endpoint_cache = endpoint_cache.clone();
            tokio::spawn(async move {
                while rx.recv().await.is_some() {
                    let mut cache = endpoint_cache.write().unwrap();
                    cache.clear();
                }
            });
        }

        Self {
            common_config,
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
    #[allow(clippy::too_many_arguments)]
    pub async fn forward_http_request_to_pipeline(
        common_config: &CommonConfig,
        client: &awc::Client,
        pipeline_name: &str,
        location: &str,
        method: Method,
        endpoint: &str,
        query_string: &str,
        timeout: Option<Duration>,
    ) -> Result<HttpResponse, ManagerError> {
        // Perform request to the pipeline
        let url = format_pipeline_url(
            if common_config.enable_https {
                "https"
            } else {
                "http"
            },
            location,
            endpoint,
            query_string,
        );
        let timeout = timeout.unwrap_or(Self::PIPELINE_HTTP_REQUEST_TIMEOUT);
        let request = client
            .request(method, &url)
            .timeout(timeout)
            .force_close()
            .with_sentry_tracing();
        let request_str = Self::format_request(&request);

        let mut original_response = request.send().await.map_err(|e| match e {
            SendRequestError::Timeout => RunnerError::PipelineInteractionUnreachable {
                pipeline_name: pipeline_name.to_string(),
                request: request_str.clone(),
                error: format_timeout_error_message(timeout, e),
            },
            SendRequestError::Connect(ConnectError::Disconnected) => {
                RunnerError::PipelineInteractionUnreachable {
                    pipeline_name: pipeline_name.to_string(),
                    request: request_str.clone(),
                    error: format_disconnected_error_message(e),
                }
            }
            _ => RunnerError::PipelineInteractionUnreachable {
                pipeline_name: pipeline_name.to_string(),
                request: request_str.clone(),
                error: format!("unable to send request due to: {e}"),
            },
        })?;
        let status = original_response.status();

        if !status.is_success() {
            info!(
                pipeline = pipeline_name,
                "HTTP request to pipeline '{pipeline_name}' returned status code {status}. Failed request: {request_str}"
            );
        }

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
        let response_body = original_response
            .body()
            .limit(RESPONSE_SIZE_LIMIT)
            .await
            .map_err(|e| RunnerError::PipelineInteractionInvalidResponse {
                pipeline_name: pipeline_name.to_string(),
                error: format!("unable to reconstruct response body due to: {e}"),
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
            &self.common_config,
            client,
            pipeline_name,
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

    pub(crate) async fn forward_websocket_request_to_pipeline_by_name(
        &self,
        awc_client: &awc::Client,
        tenant_id: TenantId,
        pipeline_name: &str,
        endpoint: &str,
        client_request: HttpRequest,
        client_body: Payload,
    ) -> Result<HttpResponse, ManagerError> {
        use awc::ws::Frame;
        use bytestring::ByteString;
        use futures_util::{SinkExt, StreamExt};

        let (location, _cache_hit) = self.check_pipeline(tenant_id, pipeline_name).await?;

        // Handle client request
        let (res, mut client_tx, client_rx) = actix_ws::handle(&client_request, client_body)
            .map_err(|e| ManagerError::ApiError {
                api_error: ApiError::UnableToConnect {
                    reason: format!("Unable to initiate websocket connection with client: {e}"),
                },
            })?;
        let mut client_rx = client_rx.max_frame_size(MAX_WS_FRAME_SIZE);

        // Connect to the pipeline
        let server_url = format_pipeline_url(
            if self.common_config.enable_https {
                "wss"
            } else {
                "ws"
            },
            &location,
            endpoint,
            client_request.query_string(),
        );
        let (_response, pipeline_conn) = awc_client
            .ws(server_url)
            .max_frame_size(MAX_WS_FRAME_SIZE)
            .connect()
            .await
            .map_err(|e| ManagerError::ApiError {
                api_error: ApiError::UnableToConnect {
                    reason: format!("Unable to initiate websocket connection with pipeline: {e}"),
                },
            })?;
        let (mut pipeline_tx, mut pipeline_rx) = pipeline_conn.split();

        // Forward backend → client using `session`
        let client_send = async move {
            while let Some(Ok(msg)) = pipeline_rx.next().await {
                match msg {
                    Frame::Text(bytes) => {
                        let maybe_text: Result<ByteString, _> = bytes.try_into();
                        if let Ok(text) = maybe_text {
                            if client_tx.text(text).await.is_err() {
                                break;
                            }
                        } else {
                            // If the conversion fails, we ignore the message
                            // as it is not a valid UTF-8 string.
                            // This shouldn't happen in practice, the pipeline
                            // should only send valid UTF-8 strings.
                            error!(
                                "Skipped invalid UTF-8 returned over web-socket as msg-type text from pipeline",
                            );
                            let _r = client_tx
                                .close(Some(CloseReason {
                                    code: CloseCode::Error,
                                    description: None,
                                }))
                                .await;
                            break;
                        }
                    }
                    Frame::Binary(bin) => {
                        if client_tx.binary(bin).await.is_err() {
                            break;
                        }
                    }
                    Frame::Ping(bytes) => {
                        if client_tx.ping(&bytes).await.is_err() {
                            break;
                        }
                    }
                    Frame::Pong(bytes) => {
                        if client_tx.pong(&bytes).await.is_err() {
                            break;
                        }
                    }
                    Frame::Close(reason) => {
                        let _ = client_tx.close(reason).await;
                        break;
                    }
                    _ => {}
                }
            }
        };

        // Spawn the task that forwards client → pipeline
        let pipeline_send = async move {
            while let Some(Ok(msg)) = client_rx.next().await {
                if pipeline_tx.send(msg).await.is_err() {
                    break;
                }
            }
        };

        // Run both tasks until one finishes
        actix_web::rt::spawn(async move {
            tokio::select! {
                _ = client_send => {},
                _ = pipeline_send => {},
            };
        });

        Ok(res)
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
        let url = format_pipeline_url(
            if self.common_config.enable_https {
                "https"
            } else {
                "http"
            },
            &location,
            endpoint,
            request.query_string(),
        );
        let timeout = timeout.unwrap_or(Self::PIPELINE_HTTP_REQUEST_TIMEOUT);
        let mut new_request = client
            .request(request.method().clone(), &url)
            .timeout(timeout)
            .force_close();

        // Add headers of the original request
        for header in request
            .headers()
            .into_iter()
            .filter(|(h, _)| *h != "connection")
        {
            new_request = new_request.append_header(header);
        }

        let new_request = new_request.with_sentry_tracing();
        let request_str = Self::format_request(&new_request);

        // Perform request to the pipeline
        let response = new_request.send_stream(body).await.map_err(|e| match e {
            SendRequestError::Timeout => RunnerError::PipelineInteractionUnreachable {
                pipeline_name: pipeline_name.to_string(),
                request: request_str.to_string(),
                error: format_timeout_error_message(timeout, e),
            },
            SendRequestError::Connect(ConnectError::Disconnected) => {
                RunnerError::PipelineInteractionUnreachable {
                    pipeline_name: pipeline_name.to_string(),
                    request: request_str.to_string(),
                    error: format_disconnected_error_message(e),
                }
            }
            _ => RunnerError::PipelineInteractionUnreachable {
                pipeline_name: pipeline_name.to_string(),
                request: request_str.to_string(),
                error: format!("unable to send request due to: {e}"),
            },
        })?;

        let status = response.status();

        if !status.is_success() {
            info!(
                pipeline = pipeline_name,
                "HTTP request to pipeline '{pipeline_name}' returned status code {status}. Failed request: {request_str}"
            );
        }

        // Build the new HTTP response with the same status, headers and streaming body
        let mut builder = HttpResponseBuilder::new(status);
        for header in response.headers().into_iter() {
            builder.append_header(header);
        }
        // Disable compression to avoid gzip frame buffering that causes clients to block
        builder.insert_header(actix_http::ContentEncoding::Identity);
        Ok(builder.streaming(response))
    }

    pub(crate) async fn get_logs_from_pipeline(
        &self,
        client: &awc::Client,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<ClientResponse<Decoder<actix_http::Payload>>, ManagerError> {
        // Retrieve pipeline
        let pipeline = self
            .db
            .lock()
            .await
            .get_pipeline_for_monitoring(tenant_id, pipeline_name)
            .await?;

        // Build request to the runner
        let url = format!(
            "{}://{}:{}/logs/{}",
            if self.common_config.enable_https {
                "https"
            } else {
                "http"
            },
            self.common_config.runner_host,
            self.common_config.runner_port,
            pipeline.id
        );

        // Perform request to the runner
        let response = client
            .request(Method::GET, &url)
            .timeout(Self::RUNNER_HTTP_REQUEST_TIMEOUT)
            .with_sentry_tracing()
            .send()
            .await
            .map_err(|e| match e {
                SendRequestError::Timeout => RunnerError::RunnerInteractionUnreachable {
                    error: format_runner_timeout_error_message(
                        Self::RUNNER_HTTP_REQUEST_TIMEOUT,
                        e,
                    ),
                },
                _ => RunnerError::RunnerInteractionUnreachable {
                    error: format!("unable to send request due to: {e}"),
                },
            })?;

        Ok(response)
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
        // Perform request to the runner
        let response = self
            .get_logs_from_pipeline(client, tenant_id, pipeline_name)
            .await?;

        // Build the HTTP response with the same status, headers and streaming body
        let mut builder = HttpResponseBuilder::new(response.status());
        for header in response.headers().into_iter() {
            builder.append_header(header);
        }
        // Disable compression to avoid gzip frame buffering that causes clients to block
        builder.insert_header(actix_http::ContentEncoding::Identity);
        Ok(builder.streaming(response))
    }

    /// Format HTTP request for logging.
    fn format_request(request: &ClientRequest) -> String {
        format!(
            "{:?} {} {}",
            request.get_version(),
            request.get_method(),
            request.get_uri()
        )
    }
}
