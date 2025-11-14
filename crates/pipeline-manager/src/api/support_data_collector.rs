use crate::api::endpoints::config::Configuration;
use crate::api::error::ApiError;
use crate::api::main::ServerState;
use crate::db::error::DBError;
use crate::db::operations::pipeline::{
    cleanup_old_support_data_collections, store_support_data_collection,
};
use crate::db::storage::Storage;
use crate::db::types::combined_status::CombinedStatus;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::tenant::TenantId;
use crate::error::ManagerError;
use actix_web::http::Method;
use actix_web::rt::time::timeout;
use actix_web::HttpResponse;
use awc::Client;
use chrono::{DateTime, Utc};
use feldera_types::error::ErrorResponse;
use futures_util::StreamExt;
use log::{debug, error, info};
use serde::Deserialize;
use std::cmp::min;
use std::collections::BTreeMap;
use std::io::Write;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::time::{sleep, Duration, Instant};
use utoipa::{IntoParams, ToSchema};

const COLLECTION_TIMEOUT: Duration = Duration::from_secs(120);

type BundleResult<T> = Result<T, String>;

fn collect() -> bool {
    true
}

/// Query parameters to control support bundle data collection.
#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct SupportBundleParameters {
    /// Whether to collect circuit profile data (default: true)
    #[serde(default = "collect")]
    pub circuit_profile: bool,

    /// Whether to collect heap profile data (default: true)
    #[serde(default = "collect")]
    pub heap_profile: bool,

    /// Whether to collect metrics data (default: true)
    #[serde(default = "collect")]
    pub metrics: bool,

    /// Whether to collect logs data (default: true)
    #[serde(default = "collect")]
    pub logs: bool,

    /// Whether to collect stats data (default: true)
    #[serde(default = "collect")]
    pub stats: bool,

    /// Whether to collect pipeline configuration data (default: true)
    #[serde(default = "collect")]
    pub pipeline_config: bool,

    /// Whether to collect system configuration data (default: true)
    #[serde(default = "collect")]
    pub system_config: bool,
}

impl Default for SupportBundleParameters {
    fn default() -> Self {
        Self {
            circuit_profile: true,
            heap_profile: true,
            metrics: true,
            logs: true,
            stats: true,
            pipeline_config: true,
            system_config: true,
        }
    }
}

/// Fetch data from a pipeline endpoint
async fn fetch_pipeline_data(
    state: &ServerState,
    client: &awc::Client,
    tenant_id: TenantId,
    pipeline_name: &str,
    endpoint: &str,
    timeout_duration: Option<Duration>,
) -> Result<HttpResponse, ManagerError> {
    state
        .runner
        .forward_http_request_to_pipeline_by_name(
            client,
            tenant_id,
            pipeline_name,
            Method::GET,
            endpoint,
            "",
            timeout_duration,
        )
        .await
}

/// Stream logs from the pipeline with timeout-based termination
async fn collect_pipeline_logs(
    state: &ServerState,
    client: &awc::Client,
    tenant_id: TenantId,
    pipeline_name: &str,
) -> Result<String, ManagerError> {
    let mut first_line = true;
    let next_line_timeout = Duration::from_millis(500);
    let mut logs = String::with_capacity(4096);

    let response = state
        .runner
        .get_logs_from_pipeline(client, tenant_id, pipeline_name)
        .await?;

    let mut response = response;
    while let Ok(Some(chunk)) = timeout(
        if first_line {
            COLLECTION_TIMEOUT
        } else {
            next_line_timeout
        },
        response.next(),
    )
    .await
    {
        match chunk {
            Ok(chunk) => {
                let text = String::from_utf8_lossy(&chunk);
                logs.push_str(&text);
                first_line = false;
            }
            Err(e) => {
                logs.push_str(&format!("ERROR: Unable to read server response: {}", e));
            }
        }
    }

    Ok(logs)
}

/// Prettify JSON data for better readability in support bundles.
fn json_prettify(data: Vec<u8>) -> Vec<u8> {
    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&data) {
        serde_json::to_vec_pretty(&json).unwrap_or(data)
    } else {
        data
    }
}

/// Compress data to .gz
fn gz_compress(data: Vec<u8>) -> Result<Vec<u8>, String> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&data).map_err(|e| e.to_string())?;
    encoder.finish().map_err(|e| e.to_string())
}

/// Convert the HTTP response from a pipeline to a bundle result or
/// a string error message in case of an error.
async fn response_to_bundle_result(
    response: Result<HttpResponse, ManagerError>,
) -> BundleResult<Vec<u8>> {
    match response {
        Ok(response) if response.status().is_success() => {
            actix_web::body::to_bytes(response.into_body())
                .await
                .map(|bytes| bytes.to_vec())
                .map_err(|e| e.to_string())
        }
        Ok(response) => {
            let mut error_string = format!("HTTP {}", response.status());
            let body = response.into_body();
            let body_bytes = actix_web::body::to_bytes(body).await.unwrap_or_default();
            if !body_bytes.is_empty() {
                let body_str = String::from_utf8_lossy(&body_bytes);
                if let Ok(error_response) = serde_json::from_str::<ErrorResponse>(&body_str) {
                    error_string.push_str(&format!(
                        ": {}",
                        error_response.message.trim_end_matches('\n')
                    ));
                } else {
                    error_string.push_str(&format!(": {}", body_str.trim_end_matches('\n')));
                }
            }
            Err(error_string)
        }
        Err(e) => Err(e.to_string()),
    }
}

/// Support bundle data collected from various sources at a single point in time.
#[derive(Debug, serde::Serialize, Eq, PartialEq)]
pub struct SupportBundleData {
    /// Version of the support bundle data format
    #[serde(default = "SupportBundleData::current_version")]
    pub version: u32,
    pub time: DateTime<Utc>,
    pub circuit_profile: BundleResult<Vec<u8>>,
    pub json_circuit_profile: BundleResult<Vec<u8>>,
    pub heap_profile: BundleResult<Vec<u8>>,
    pub metrics: BundleResult<Vec<u8>>,
    pub logs: BundleResult<String>,
    pub stats: BundleResult<Vec<u8>>,
    pub pipeline_config: BundleResult<Vec<u8>>,
    pub system_config: BundleResult<Vec<u8>>,
}

impl SupportBundleData {
    /// Current version of the support bundle data format
    pub const fn current_version() -> u32 {
        3
    }

    #[cfg(test)]
    pub(crate) fn test_data() -> Self {
        Self {
            version: 3,
            time: chrono::Utc::now(),
            circuit_profile: Ok(vec![1, 2, 3]),
            json_circuit_profile: Ok(vec![1, 2, 3]),
            heap_profile: Ok(vec![4, 5, 6]),
            metrics: Ok(vec![7, 8, 9]),
            logs: Ok("test logs".to_string()),
            stats: Ok(vec![10, 11, 12]),
            pipeline_config: Ok(vec![13, 14, 15]),
            system_config: Ok(vec![16, 17, 18]),
        }
    }
}

impl<'de> serde::Deserialize<'de> for SupportBundleData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Helper function to create a default error for missing fields
        fn missing_field_error<T>() -> BundleResult<T> {
            Err("Missing from data source (field was added in newer version)".to_string())
        }

        #[derive(serde::Deserialize)]
        struct Helper {
            #[serde(default = "SupportBundleData::current_version")]
            version: u32,
            time: DateTime<Utc>,
            #[serde(default)]
            circuit_profile: Option<BundleResult<Vec<u8>>>,
            #[serde(default)]
            json_circuit_profile: Option<BundleResult<Vec<u8>>>,
            #[serde(default)]
            heap_profile: Option<BundleResult<Vec<u8>>>,
            #[serde(default)]
            metrics: Option<BundleResult<Vec<u8>>>,
            #[serde(default)]
            logs: Option<BundleResult<String>>,
            #[serde(default)]
            stats: Option<BundleResult<Vec<u8>>>,
            #[serde(default)]
            pipeline_config: Option<BundleResult<Vec<u8>>>,
            #[serde(default)]
            system_config: Option<BundleResult<Vec<u8>>>,
        }

        let helper = Helper::deserialize(deserializer)?;

        Ok(SupportBundleData {
            version: helper.version,
            time: helper.time,
            circuit_profile: helper.circuit_profile.unwrap_or_else(missing_field_error),
            json_circuit_profile: helper
                .json_circuit_profile
                .unwrap_or_else(missing_field_error),
            heap_profile: helper.heap_profile.unwrap_or_else(missing_field_error),
            metrics: helper.metrics.unwrap_or_else(missing_field_error),
            logs: helper.logs.unwrap_or_else(missing_field_error),
            stats: helper.stats.unwrap_or_else(missing_field_error),
            pipeline_config: helper.pipeline_config.unwrap_or_else(missing_field_error),
            system_config: helper.system_config.unwrap_or_else(missing_field_error),
        })
    }
}

impl SupportBundleData {
    pub(crate) async fn collect(
        state: &ServerState,
        client: &awc::Client,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<Self, ManagerError> {
        let (
            circuit_profile,
            json_circuit_profile,
            heap_profile,
            metrics,
            logs,
            stats,
            pipeline_config,
            system_config,
        ) = tokio::join!(
            Self::collect_circuit_profile(state, client, tenant_id, pipeline_name,),
            Self::collect_json_circuit_profile(state, client, tenant_id, pipeline_name,),
            Self::collect_heap_profile(state, client, tenant_id, pipeline_name,),
            Self::collect_metrics(state, client, tenant_id, pipeline_name),
            Self::collect_logs(state, client, tenant_id, pipeline_name),
            Self::collect_stats(state, client, tenant_id, pipeline_name),
            Self::collect_pipeline_config(state, tenant_id, pipeline_name),
            Self::collect_system_config(state),
        );

        Ok(SupportBundleData {
            version: Self::current_version(),
            time: chrono::Utc::now(),
            circuit_profile,
            json_circuit_profile,
            heap_profile,
            metrics,
            logs,
            stats,
            pipeline_config,
            system_config,
        })
    }

    async fn collect_circuit_profile(
        state: &ServerState,
        client: &awc::Client,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> BundleResult<Vec<u8>> {
        let response = fetch_pipeline_data(
            state,
            client,
            tenant_id,
            pipeline_name,
            "dump_profile",
            Some(COLLECTION_TIMEOUT),
        )
        .await;

        response_to_bundle_result(response).await
    }

    async fn collect_json_circuit_profile(
        state: &ServerState,
        client: &awc::Client,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> BundleResult<Vec<u8>> {
        let response = fetch_pipeline_data(
            state,
            client,
            tenant_id,
            pipeline_name,
            "dump_json_profile",
            Some(COLLECTION_TIMEOUT),
        )
        .await;

        response_to_bundle_result(response).await
    }

    async fn collect_heap_profile(
        state: &ServerState,
        client: &awc::Client,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> BundleResult<Vec<u8>> {
        let response = fetch_pipeline_data(
            state,
            client,
            tenant_id,
            pipeline_name,
            "heap_profile",
            Some(COLLECTION_TIMEOUT),
        )
        .await;

        response_to_bundle_result(response).await
    }

    async fn collect_metrics(
        state: &ServerState,
        client: &awc::Client,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> BundleResult<Vec<u8>> {
        let response = fetch_pipeline_data(
            state,
            client,
            tenant_id,
            pipeline_name,
            "metrics",
            Some(COLLECTION_TIMEOUT),
        )
        .await;

        response_to_bundle_result(response).await
    }

    async fn collect_logs(
        state: &ServerState,
        client: &awc::Client,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> BundleResult<String> {
        collect_pipeline_logs(state, client, tenant_id, pipeline_name)
            .await
            .map_err(|e| e.to_string())
    }

    async fn collect_stats(
        state: &ServerState,
        client: &awc::Client,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> BundleResult<Vec<u8>> {
        let response = fetch_pipeline_data(
            state,
            client,
            tenant_id,
            pipeline_name,
            "stats",
            Some(COLLECTION_TIMEOUT),
        )
        .await;

        response_to_bundle_result(response).await.map(json_prettify)
    }

    async fn collect_pipeline_config(
        state: &ServerState,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> BundleResult<Vec<u8>> {
        Self::get_pipeline_configuration(state, tenant_id, pipeline_name)
            .await
            .map_err(|e| e.to_string())
            .and_then(gz_compress)
    }

    async fn collect_system_config(state: &ServerState) -> BundleResult<Vec<u8>> {
        Self::get_system_configuration(state)
            .await
            .map_err(|e| e.to_string())
    }

    #[allow(clippy::type_complexity)]
    pub(crate) async fn push_to_zip(
        &self,
        add_to_zip: &mut dyn FnMut(&str, &[u8]) -> Result<(), Box<dyn std::error::Error>>,
        params: &SupportBundleParameters,
    ) -> Result<(Vec<String>, Vec<String>), Box<dyn std::error::Error>> {
        let mut manifest_entries = Vec::new();
        let mut error_entries = Vec::new();

        // Add circuit profile
        if params.circuit_profile {
            match &self.circuit_profile {
                Ok(content) => {
                    let _ = add_to_zip("circuit_profile.zip", content);
                    manifest_entries.push("✓ circuit_profile.zip".to_string());
                }
                Err(e) => {
                    error_entries.push(format!("✗ circuit_profile.zip: {}", e));
                }
            };
            match &self.json_circuit_profile {
                Ok(content) => {
                    let _ = add_to_zip("json_circuit_profile.zip", content);
                    manifest_entries.push("✓ json_circuit_profile.zip".to_string());
                }
                Err(e) => {
                    error_entries.push(format!("✗ json_circuit_profile.zip: {}", e));
                }
            }
        } else {
            error_entries.push("⚠ circuit_profile.zip: Skipped due to user input".to_string());
        }

        // Add heap profile
        if params.heap_profile {
            match &self.heap_profile {
                Ok(content) => {
                    let _ = add_to_zip("heap_profile.pb.gz", content);
                    manifest_entries.push("✓ heap_profile.pb.gz".to_string());
                }
                Err(e) => {
                    error_entries.push(format!("✗ heap_profile.pb.gz: {}", e));
                }
            }
        } else {
            error_entries.push("⚠ heap_profile.pb.gz: Skipped due to user input".to_string());
        }

        // Add metrics
        if params.metrics {
            match &self.metrics {
                Ok(content) => {
                    let _ = add_to_zip("metrics.txt", content);
                    manifest_entries.push("✓ metrics.txt".to_string());
                }
                Err(e) => {
                    error_entries.push(format!("✗ metrics.txt: {}", e));
                }
            }
        } else {
            error_entries.push("⚠ metrics.txt: Skipped due to user input".to_string());
        }

        // Add logs
        if params.logs {
            match &self.logs {
                Ok(content) => {
                    let _ = add_to_zip("logs.txt", content.as_bytes());
                    manifest_entries.push("✓ logs.txt".to_string());
                }
                Err(e) => {
                    error_entries.push(format!("✗ logs.txt: {}", e));
                }
            }
        } else {
            error_entries.push("⚠ logs.txt: Skipped due to user input".to_string());
        }

        // Add stats
        if params.stats {
            match &self.stats {
                Ok(content) => {
                    let _ = add_to_zip("stats.json", content);
                    manifest_entries.push("✓ stats.json".to_string());
                }
                Err(e) => {
                    error_entries.push(format!("✗ stats.json: {}", e));
                }
            }
        } else {
            error_entries.push("⚠ stats.json: Skipped due to user input".to_string());
        }

        // Add pipeline config
        if params.pipeline_config {
            match &self.pipeline_config {
                Ok(content) => {
                    if self.version == 1 {
                        let _ = add_to_zip("pipeline_config.json", content);
                        manifest_entries.push("✓ pipeline_config.json".to_string());
                    } else {
                        let _ = add_to_zip("pipeline_config.json.gz", content);
                        manifest_entries.push("✓ pipeline_config.json.gz".to_string());
                    }
                }
                Err(e) => {
                    if self.version == 1 {
                        error_entries.push(format!("✗ pipeline_config.json: {}", e));
                    } else {
                        error_entries.push(format!("✗ pipeline_config.json.gz: {}", e));
                    }
                }
            }
        } else {
            error_entries.push("⚠ pipeline_config.json: Skipped due to user input".to_string());
        }

        // Add system config
        if params.system_config {
            match &self.system_config {
                Ok(content) => {
                    let _ = add_to_zip("system_config.json", content);
                    manifest_entries.push("✓ system_config.json".to_string());
                }
                Err(e) => {
                    error_entries.push(format!("✗ system_config.json: {}", e));
                }
            }
        } else {
            error_entries.push("⚠ system_config.json: Skipped due to user input".to_string());
        }

        Ok((manifest_entries, error_entries))
    }

    /// Get pipeline configuration from local database
    async fn get_pipeline_configuration(
        state: &ServerState,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<Vec<u8>, ManagerError> {
        let pipeline = state
            .db
            .lock()
            .await
            .get_pipeline(tenant_id, pipeline_name)
            .await?;

        serde_json::to_vec_pretty(&pipeline).map_err(|e| {
            ManagerError::from(ApiError::UnableToCreateSupportBundle {
                reason: format!("Failed to serialize pipeline config: {}", e),
            })
        })
    }

    /// Get system configuration
    async fn get_system_configuration(state: &ServerState) -> Result<Vec<u8>, ManagerError> {
        let config = Configuration::gather(state).await;
        serde_json::to_vec_pretty(&config).map_err(|e| {
            ManagerError::from(ApiError::UnableToCreateSupportBundle {
                reason: format!("Failed to serialize system config: {}", e),
            })
        })
    }
}

/// Entry in the collection schedule
#[derive(Debug, Clone)]
struct CollectionScheduleEntry {
    /// Pipeline ID
    pipeline_id: PipelineId,
    /// Tenant ID
    tenant_id: TenantId,
    /// Next collection time
    #[allow(dead_code)]
    next_collection: Instant,
}

/// Support data collector that manages collection scheduling and execution
pub struct SupportDataCollector {
    /// Database connection
    state: Arc<ServerState>,
    /// Collection frequency in minutes
    collection_frequency: u64,
    /// Number of collections to retain per pipeline
    retention_count: u64,
    /// Collection schedule ordered by next collection time
    schedule: BTreeMap<Instant, CollectionScheduleEntry>,
    /// HTTP client for making requests to pipelines
    http_client: Client,
    /// Shutdown signal receiver
    shutdown_rx: watch::Receiver<bool>,
}

enum PostCollectionAction {
    Reschedule,
    Remove,
}

impl SupportDataCollector {
    /// Create a new support data collector
    pub(crate) fn new(
        state: Arc<ServerState>,
        client: Client,
        collection_frequency: u64,
        retention_count: u64,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            state,
            collection_frequency,
            retention_count,
            schedule: BTreeMap::new(),
            http_client: client,
            shutdown_rx,
        }
    }

    /// Start the support data collection task
    pub async fn run(mut self) {
        info!(
            "Starting support data collector with frequency of {} seconds and retention of {} collections",
            self.collection_frequency, self.retention_count
        );

        let mut backoff_duration = Duration::from_secs(30);
        loop {
            if *self.shutdown_rx.borrow() {
                info!("Support data collector shutting down (signal)");
                break;
            }

            if let Err(e) = self.run_iteration().await {
                error!("Support data collector iteration failed: {}", e);
                // Sleep for a short time before retrying, but exit early on shutdown
                if self.sleep_or_shutdown(backoff_duration).await {
                    info!("Support data collector shutting down (post-error)");
                    break;
                }
                backoff_duration = min(Duration::from_secs(180), backoff_duration * 2);
            } else {
                backoff_duration = Duration::from_secs(30);
            }
        }
    }

    /// Run one iteration of the collection loop
    async fn run_iteration(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Find the next wakeup time
        let next_wakeup = self.schedule.keys().next().copied();

        if let Some(wakeup_time) = next_wakeup {
            let now = Instant::now();
            if wakeup_time > now {
                let sleep_duration = wakeup_time.duration_since(now);
                debug!("Sleeping for {:?} until next collection", sleep_duration);
                if self.sleep_or_shutdown(sleep_duration).await {
                    // Graceful shutdown requested
                    return Ok(());
                }
            }
        } else {
            // No scheduled collections, sleep for the collection frequency
            let sleep_duration = Duration::from_secs(self.collection_frequency);
            debug!(
                "No scheduled collections, sleeping for {:?}",
                sleep_duration
            );
            if self.sleep_or_shutdown(sleep_duration).await {
                // Graceful shutdown requested
                return Ok(());
            }
        }

        // Check for new pipelines and collect data for due pipelines
        self.check_for_new_pipelines().await?;
        self.collect_due_pipelines().await?;

        Ok(())
    }

    /// Sleep for the specified duration or return early if shutdown is requested.
    /// Returns true if shutdown was requested, false if the sleep completed.
    async fn sleep_or_shutdown(&mut self, duration: Duration) -> bool {
        if *self.shutdown_rx.borrow() {
            return true;
        }
        tokio::select! {
            _ = sleep(duration) => {
                false
            }
            _ = self.shutdown_rx.changed() => {
                true
            }
        }
    }

    /// Check for new pipelines and add them to the schedule
    async fn check_for_new_pipelines(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Get all running pipelines that aren't already scheduled
        let running_pipelines = self
            .state
            .db
            .lock()
            .await
            .list_pipelines_across_all_tenants_for_monitoring()
            .await?;

        for (tenant_id, pipeline) in running_pipelines {
            // Only collect data for pipelines that are actually running
            let combined_status = CombinedStatus::new(
                pipeline.deployment_resources_status,
                pipeline.deployment_runtime_status,
            );
            if combined_status == CombinedStatus::Running {
                let pipeline_id = pipeline.id;

                // Check if this pipeline is already scheduled
                // This is O(n) but (probably?) not a big deal.
                let is_scheduled = self
                    .schedule
                    .values()
                    .any(|entry| entry.pipeline_id == pipeline_id);

                if !is_scheduled {
                    let next_collection = Instant::now();
                    let entry = CollectionScheduleEntry {
                        pipeline_id,
                        tenant_id,
                        next_collection,
                    };

                    self.schedule.insert(next_collection, entry);
                    debug!(
                        "Added support data collection for pipeline {} to schedule.",
                        pipeline_id
                    );
                }
            }
        }

        Ok(())
    }

    /// Collect support data for pipelines that are due for collection
    async fn collect_due_pipelines(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let now = Instant::now();
        let mut to_collect = Vec::new();

        // Find all pipelines that are due for collection
        while let Some((&wakeup_time, entry)) = self.schedule.first_key_value() {
            if wakeup_time <= now {
                to_collect.push(entry.clone());
                self.schedule.remove(&wakeup_time);
            } else {
                break;
            }
        }

        // Collect data for each due pipeline
        for entry in to_collect {
            match self.collect_pipeline_data(&entry).await {
                Ok(PostCollectionAction::Reschedule) => {
                    // Schedule next collection at `collection_frequency` seconds from now.
                    let next_collection =
                        Instant::now() + Duration::from_secs(self.collection_frequency);
                    let next_entry = CollectionScheduleEntry {
                        pipeline_id: entry.pipeline_id,
                        tenant_id: entry.tenant_id,
                        next_collection,
                    };
                    self.schedule.insert(next_collection, next_entry);
                }
                Err(e) => {
                    error!(
                        "Failed to collect support data for pipeline {}: {}",
                        entry.pipeline_id, e
                    );

                    // Try again in a minute.
                    let backoff_duration = Duration::from_secs(60);
                    let next_collection = Instant::now() + backoff_duration;
                    let rescheduled_entry = CollectionScheduleEntry {
                        pipeline_id: entry.pipeline_id,
                        tenant_id: entry.tenant_id,
                        next_collection,
                    };
                    self.schedule.insert(next_collection, rescheduled_entry);
                }
                Ok(PostCollectionAction::Remove) => {}
            }
        }

        Ok(())
    }

    /// Collect support data for a specific pipeline
    async fn collect_pipeline_data(
        &self,
        entry: &CollectionScheduleEntry,
    ) -> Result<PostCollectionAction, Box<dyn std::error::Error + Send + Sync>> {
        let pipeline = match self
            .state
            .db
            .lock()
            .await
            .get_pipeline_by_id_for_monitoring(entry.tenant_id, entry.pipeline_id)
            .await
        {
            Ok(pipeline) => pipeline,
            Err(DBError::UnknownPipeline { .. }) => {
                debug!(
                    "Removing {} from support data collection schedule (pipeline deleted)",
                    entry.pipeline_id
                );
                return Ok(PostCollectionAction::Remove);
            }
            Err(e) => {
                error!("Failed to get pipeline {}: {}", entry.pipeline_id, e);
                return Err(e.into());
            }
        };

        // Only collect data for running pipelines
        let combined_status = CombinedStatus::new(
            pipeline.deployment_resources_status,
            pipeline.deployment_runtime_status,
        );
        if combined_status != CombinedStatus::Running {
            debug!(
                "Removing {} from support data collection schedule (status change to: {:?})",
                entry.pipeline_id, combined_status
            );
            return Ok(PostCollectionAction::Remove);
        }

        // Collect support data
        let support_data = self
            .collect_support_data(entry.tenant_id, &pipeline)
            .await?;
        self.store_support_data(entry.pipeline_id, entry.tenant_id, &support_data)
            .await?;
        if let Err(err) = self.cleanup_old_collections(entry.pipeline_id).await {
            error!(
                "Failed to cleanup old support data collections for pipeline {}: {}",
                entry.pipeline_id, err
            );
        }

        debug!(
            "Collected support data for pipeline {}, reschedule",
            entry.pipeline_id
        );

        Ok(PostCollectionAction::Reschedule)
    }

    /// Collect support data for a pipeline
    async fn collect_support_data(
        &self,
        tenant_id: TenantId,
        pipeline: &crate::db::types::pipeline::ExtendedPipelineDescrMonitoring,
    ) -> Result<SupportBundleData, Box<dyn std::error::Error + Send + Sync>> {
        SupportBundleData::collect(
            &self.state,
            &self.http_client,
            tenant_id,
            pipeline.name.as_str(),
        )
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    /// Store support data in the database
    async fn store_support_data(
        &self,
        pipeline_id: PipelineId,
        tenant_id: TenantId,
        support_bundle: &SupportBundleData,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Get a client from the pool and create a transaction
        let mut client = self.state.db.lock().await.pool.get().await?;
        let txn = client.transaction().await?;

        store_support_data_collection(&txn, pipeline_id, tenant_id, support_bundle).await?;

        txn.commit().await?;
        Ok(())
    }

    /// Clean up old support data collections
    async fn cleanup_old_collections(
        &self,
        pipeline_id: PipelineId,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut client = self.state.db.lock().await.pool.get().await?;
        let txn = client.transaction().await?;
        let r =
            cleanup_old_support_data_collections(&txn, pipeline_id, self.retention_count as i64)
                .await?;
        txn.commit().await?;
        if r != 0 {
            debug!(
                "Cleaned up {} old support data collection(s) for pipeline {}",
                r, pipeline_id
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::TenantRecord;
    use crate::db::test::setup_pg;
    use crate::db::types::pipeline::PipelineDescr;
    use crate::db::types::program::{RustCompilationInfo, SqlCompilationInfo};
    use crate::db::types::version::Version;
    use feldera_types::runtime_status::{
        BootstrapPolicy, ExtendedRuntimeStatus, RuntimeDesiredStatus, RuntimeStatus,
    };
    use serde_json::json;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio::time::{sleep, Duration};
    use uuid::Uuid;

    #[test]
    fn serialization_roundtrip() {
        let original = SupportBundleData {
            version: 1,
            time: chrono::Utc::now(),
            circuit_profile: Ok(vec![1, 2, 3]),
            json_circuit_profile: Ok(vec![1, 2, 3]),
            heap_profile: Err("test error".to_string()),
            metrics: Ok(vec![4, 5, 6]),
            logs: Ok("test logs".to_string()),
            stats: Err("stats error".to_string()),
            pipeline_config: Ok(vec![7, 8, 9]),
            system_config: Ok(vec![10, 11, 12]),
        };

        let serialized = rmp_serde::to_vec(&original).unwrap();
        let deserialized: SupportBundleData = rmp_serde::from_slice(&serialized).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn missing_fields_handling() {
        #[derive(Debug, serde::Serialize)]
        struct LossySupportBundleData {
            version: u32,
            time: DateTime<Utc>,
        }
        let missing_fields_bundle = LossySupportBundleData {
            version: 0,
            time: chrono::Utc::now(),
        };
        let serialized = rmp_serde::to_vec(&missing_fields_bundle).unwrap();
        let deserialized: SupportBundleData = rmp_serde::from_slice(&serialized).unwrap();

        assert_eq!(deserialized.version, 0);
        assert_eq!(
            deserialized.circuit_profile,
            Err("Missing from data source (field was added in newer version)".to_string())
        );
        assert_eq!(
            deserialized.json_circuit_profile,
            Err("Missing from data source (field was added in newer version)".to_string())
        );
        assert_eq!(
            deserialized.heap_profile,
            Err("Missing from data source (field was added in newer version)".to_string())
        );
        assert_eq!(
            deserialized.metrics,
            Err("Missing from data source (field was added in newer version)".to_string())
        );
        assert_eq!(
            deserialized.logs,
            Err("Missing from data source (field was added in newer version)".to_string())
        );
        assert_eq!(
            deserialized.stats,
            Err("Missing from data source (field was added in newer version)".to_string())
        );
        assert_eq!(
            deserialized.pipeline_config,
            Err("Missing from data source (field was added in newer version)".to_string())
        );
        assert_eq!(
            deserialized.system_config,
            Err("Missing from data source (field was added in newer version)".to_string())
        );
    }

    #[tokio::test]
    async fn push_to_zip_with_ignores() {
        let bundle_data = SupportBundleData {
            version: 1,
            time: chrono::Utc::now(),
            circuit_profile: Ok(vec![1, 2, 3]),
            json_circuit_profile: Ok(vec![1, 2, 3]),
            heap_profile: Ok(vec![4, 5, 6]),
            metrics: Ok(vec![7, 8, 9]),
            logs: Ok("test logs".to_string()),
            stats: Ok(vec![10, 11, 12]),
            pipeline_config: Ok(vec![13, 14, 15]),
            system_config: Ok(vec![16, 17, 18]),
        };

        // Test with all parameters enabled
        let all_enabled = SupportBundleParameters::default();
        let mut zip_entries = Vec::new();

        let (manifest, errors) = bundle_data
            .push_to_zip(
                &mut |filename, content| {
                    zip_entries.push((filename.to_string(), content.to_vec()));
                    Ok(())
                },
                &all_enabled,
            )
            .await
            .unwrap();

        assert_eq!(manifest.len(), 8);
        assert_eq!(errors.len(), 0);
        assert_eq!(zip_entries.len(), 8);

        // Test with some parameters disabled
        let some_disabled = SupportBundleParameters {
            circuit_profile: false,
            heap_profile: true,
            metrics: false,
            logs: true,
            stats: false,
            pipeline_config: true,
            system_config: false,
        };

        zip_entries.clear();

        let (manifest, errors) = bundle_data
            .push_to_zip(
                &mut |filename, content| {
                    zip_entries.push((filename.to_string(), content.to_vec()));
                    Ok(())
                },
                &some_disabled,
            )
            .await
            .unwrap();

        assert_eq!(manifest.len(), 3);
        assert_eq!(errors.len(), 4);
        assert_eq!(zip_entries.len(), 3);
    }

    #[tokio::test]
    async fn support_bundle_collections_deleted_when_pipeline_deleted() {
        // Setup test database and server state
        crate::ensure_default_crypto_provider();
        let (db, _temp_dir) = setup_pg().await;
        let db: Arc<Mutex<crate::db::storage_postgres::StoragePostgres>> = Arc::new(Mutex::new(db));
        let state: Arc<ServerState> = Arc::new(ServerState::test_state(db.clone()).await);

        // Create a tenant and pipeline
        let tenant_record = TenantRecord::default();
        let tenant_id = tenant_record.id;
        let pipeline_id = PipelineId(Uuid::now_v7());

        // Create a pipeline
        db.lock()
            .await
            .new_pipeline(tenant_id, pipeline_id.0, "v0", PipelineDescr::test_descr())
            .await
            .unwrap();

        // Set program status to success (required for deployment)
        db.lock()
            .await
            .transit_program_status_to_compiling_sql(tenant_id, pipeline_id, Version(1))
            .await
            .unwrap();
        db.lock()
            .await
            .transit_program_status_to_sql_compiled(
                tenant_id,
                pipeline_id,
                Version(1),
                &SqlCompilationInfo::success(),
                &json!({
                    "schema": {
                        "inputs": [],
                        "outputs": []
                    },
                    "input_connectors": {},
                    "output_connectors": {},
                }),
            )
            .await
            .unwrap();
        db.lock()
            .await
            .transit_program_status_to_compiling_rust(tenant_id, pipeline_id, Version(1))
            .await
            .unwrap();
        db.lock()
            .await
            .transit_program_status_to_success(
                tenant_id,
                pipeline_id,
                Version(1),
                &RustCompilationInfo::success(),
                "test-checksum",
                "test-integrity-checksum",
            )
            .await
            .unwrap();

        // Set deployment status to running
        db.lock()
            .await
            .set_deployment_resources_desired_status_provisioned(
                tenant_id,
                "test_pipeline",
                RuntimeDesiredStatus::Running,
                BootstrapPolicy::default(),
            )
            .await
            .unwrap();
        db.lock()
            .await
            .transit_deployment_resources_status_to_provisioning(
                tenant_id,
                pipeline_id,
                Version(1),
                Uuid::nil(),
                json!({
                    "inputs": {}
                }),
            )
            .await
            .unwrap();
        db.lock()
            .await
            .transit_deployment_resources_status_to_provisioned(
                tenant_id,
                pipeline_id,
                Version(1),
                "test-location",
                ExtendedRuntimeStatus {
                    runtime_status: RuntimeStatus::Initializing,
                    runtime_status_details: json!(""),
                    runtime_desired_status: RuntimeDesiredStatus::Running,
                },
            )
            .await
            .unwrap();
        db.lock()
            .await
            .transit_deployment_resources_status_to_provisioned(
                tenant_id,
                pipeline_id,
                Version(1),
                "test-location",
                ExtendedRuntimeStatus {
                    runtime_status: RuntimeStatus::Running,
                    runtime_status_details: json!(""),
                    runtime_desired_status: RuntimeDesiredStatus::Running,
                },
            )
            .await
            .unwrap();

        // Create support data collector
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let http_client = awc::Client::default();
        let mut collector =
            SupportDataCollector::new(state.clone(), http_client, 1, 2, shutdown_rx);

        // Run one iteration to add pipeline to schedule
        collector.check_for_new_pipelines().await.unwrap();

        // Verify pipeline is scheduled
        assert_eq!(collector.schedule.len(), 1);
        let scheduled_pipeline_id = collector.schedule.values().next().unwrap().pipeline_id;
        assert_eq!(scheduled_pipeline_id, pipeline_id);

        // Store support bundle data directly
        collector
            .store_support_data(pipeline_id, tenant_id, &SupportBundleData::test_data())
            .await
            .unwrap();

        // Verify support bundle data was stored
        let mut client = db.lock().await.pool.get().await.unwrap();
        let txn = client.transaction().await.unwrap();
        let bundles =
            crate::db::operations::pipeline::get_support_bundle_data(&txn, pipeline_id, 10)
                .await
                .unwrap();
        txn.commit().await.unwrap();
        assert_eq!(bundles.len(), 1);

        // Stop and clear the pipeline
        db.lock()
            .await
            .set_deployment_resources_desired_status_stopped(tenant_id, "test_pipeline")
            .await
            .unwrap();
        db.lock()
            .await
            .transit_deployment_resources_status_to_stopping(
                tenant_id,
                pipeline_id,
                Version(1),
                None,
                None,
            )
            .await
            .unwrap();
        db.lock()
            .await
            .transit_deployment_resources_status_to_stopped(tenant_id, pipeline_id, Version(1))
            .await
            .unwrap();
        db.lock()
            .await
            .transit_storage_status_to_clearing_if_not_cleared(tenant_id, "test_pipeline")
            .await
            .unwrap();
        db.lock()
            .await
            .transit_storage_status_to_cleared(tenant_id, pipeline_id)
            .await
            .unwrap();
        db.lock()
            .await
            .delete_pipeline(tenant_id, "test_pipeline")
            .await
            .unwrap();

        // Verify support bundle data was deleted (due to CASCADE)
        let mut client = db.lock().await.pool.get().await.unwrap();
        let txn = client.transaction().await.unwrap();
        let bundles =
            crate::db::operations::pipeline::get_support_bundle_data(&txn, pipeline_id, 10)
                .await
                .unwrap();
        txn.commit().await.unwrap();

        assert_eq!(bundles.len(), 0);
        let _ = shutdown_tx.send(true);
    }

    #[tokio::test]
    async fn support_bundles_garbage_collected_over_time() {
        // Setup test database and server state
        crate::ensure_default_crypto_provider();
        let (db, _temp_dir) = setup_pg().await;
        let db = Arc::new(Mutex::new(db));
        let state = Arc::new(ServerState::test_state(db.clone()).await);

        // Create a tenant and pipeline
        let tenant_record = TenantRecord::default();
        let tenant_id = tenant_record.id;
        let pipeline_id = PipelineId(Uuid::now_v7());

        // Create a pipeline
        db.lock()
            .await
            .new_pipeline(tenant_id, pipeline_id.0, "v0", PipelineDescr::test_descr())
            .await
            .unwrap();

        // Set pipeline to running status
        db.lock()
            .await
            .transit_program_status_to_compiling_sql(tenant_id, pipeline_id, Version(1))
            .await
            .unwrap();
        db.lock()
            .await
            .transit_program_status_to_sql_compiled(
                tenant_id,
                pipeline_id,
                Version(1),
                &SqlCompilationInfo::success(),
                &json!({
                    "schema": {
                        "inputs": [],
                        "outputs": []
                    },
                    "input_connectors": {},
                    "output_connectors": {},
                }),
            )
            .await
            .unwrap();
        db.lock()
            .await
            .transit_program_status_to_compiling_rust(tenant_id, pipeline_id, Version(1))
            .await
            .unwrap();
        db.lock()
            .await
            .transit_program_status_to_success(
                tenant_id,
                pipeline_id,
                Version(1),
                &RustCompilationInfo::success(),
                "checksum1",
                "checksum2",
            )
            .await
            .unwrap();

        db.lock()
            .await
            .set_deployment_resources_desired_status_provisioned(
                tenant_id,
                "test_pipeline",
                RuntimeDesiredStatus::Running,
                BootstrapPolicy::default(),
            )
            .await
            .unwrap();
        db.lock()
            .await
            .transit_deployment_resources_status_to_provisioning(
                tenant_id,
                pipeline_id,
                Version(1),
                Uuid::from_u128(1),
                json!({
                    "inputs": {}
                }),
            )
            .await
            .unwrap();
        db.lock()
            .await
            .transit_deployment_resources_status_to_provisioned(
                tenant_id,
                pipeline_id,
                Version(1),
                "test-location",
                ExtendedRuntimeStatus {
                    runtime_status: RuntimeStatus::Initializing,
                    runtime_status_details: json!(""),
                    runtime_desired_status: RuntimeDesiredStatus::Running,
                },
            )
            .await
            .unwrap();
        db.lock()
            .await
            .transit_deployment_resources_status_to_provisioned(
                tenant_id,
                pipeline_id,
                Version(1),
                "test-location",
                ExtendedRuntimeStatus {
                    runtime_status: RuntimeStatus::Running,
                    runtime_status_details: json!(""),
                    runtime_desired_status: RuntimeDesiredStatus::Running,
                },
            )
            .await
            .unwrap();

        // Create support data collector
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let http_client = awc::Client::default();
        let retention_count = 3;
        let collector =
            SupportDataCollector::new(state.clone(), http_client, 1, retention_count, shutdown_rx);

        // Create 5 mock support bundle data entries
        for _i in 0..5 {
            // Store support bundle data directly
            collector
                .store_support_data(pipeline_id, tenant_id, &SupportBundleData::test_data())
                .await
                .unwrap();
            // Small delay to ensure different timestamps
            sleep(Duration::from_millis(10)).await;
        }

        // Verify we have 5 collections initially
        let mut client = db.lock().await.pool.get().await.unwrap();
        let txn = client.transaction().await.unwrap();
        let bundles =
            crate::db::operations::pipeline::get_support_bundle_data(&txn, pipeline_id, 10)
                .await
                .unwrap();
        assert_eq!(bundles.len(), 5);
        txn.commit().await.unwrap();

        // Run cleanup to remove old collections
        collector
            .cleanup_old_collections(pipeline_id)
            .await
            .unwrap();

        // Verify only 3 collections remain (retention_count)
        let mut client = db.lock().await.pool.get().await.unwrap();
        let txn = client.transaction().await.unwrap();
        let bundles = crate::db::operations::pipeline::get_support_bundle_data(
            &txn,
            pipeline_id,
            retention_count + 10,
        )
        .await
        .unwrap();
        assert_eq!(bundles.len(), retention_count as usize);
        txn.commit().await.unwrap();

        // Signal shutdown
        let _ = shutdown_tx.send(true);
    }

    #[tokio::test]
    async fn pipelines_added_removed_from_collection_as_started_stopped() {
        // Setup test database and server state
        crate::ensure_default_crypto_provider();
        let (db, _temp_dir) = setup_pg().await;
        let db = Arc::new(Mutex::new(db));
        let state = Arc::new(ServerState::test_state(db.clone()).await);

        // Create a tenant and pipeline
        let tenant_record = TenantRecord::default();
        let tenant_id = tenant_record.id;
        let pipeline_id = PipelineId(Uuid::now_v7());

        // Create a pipeline
        db.lock()
            .await
            .new_pipeline(tenant_id, pipeline_id.0, "v0", PipelineDescr::test_descr())
            .await
            .unwrap();

        // Set pipeline to compiled status
        db.lock()
            .await
            .transit_program_status_to_compiling_sql(tenant_id, pipeline_id, Version(1))
            .await
            .unwrap();
        db.lock()
            .await
            .transit_program_status_to_sql_compiled(
                tenant_id,
                pipeline_id,
                Version(1),
                &SqlCompilationInfo::success(),
                &json!({
                    "schema": {
                        "inputs": [],
                        "outputs": []
                    },
                    "input_connectors": {},
                    "output_connectors": {},
                }),
            )
            .await
            .unwrap();
        db.lock()
            .await
            .transit_program_status_to_compiling_rust(tenant_id, pipeline_id, Version(1))
            .await
            .unwrap();
        db.lock()
            .await
            .transit_program_status_to_success(
                tenant_id,
                pipeline_id,
                Version(1),
                &RustCompilationInfo::success(),
                "checksum1",
                "checksum2",
            )
            .await
            .unwrap();

        // Create support data collector
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let http_client = awc::Client::default();
        let mut collector =
            SupportDataCollector::new(state.clone(), http_client, 1, 2, shutdown_rx);

        // Initially pipeline is stopped, so it shouldn't be in schedule
        collector.check_for_new_pipelines().await.unwrap();
        assert_eq!(collector.schedule.len(), 0);

        // Start the pipeline
        db.lock()
            .await
            .set_deployment_resources_desired_status_provisioned(
                tenant_id,
                "test_pipeline",
                RuntimeDesiredStatus::Running,
                BootstrapPolicy::default(),
            )
            .await
            .unwrap();

        // Still not collecting data for this pipelines
        collector.check_for_new_pipelines().await.unwrap();
        assert_eq!(collector.schedule.len(), 0);

        db.lock()
            .await
            .transit_deployment_resources_status_to_provisioning(
                tenant_id,
                pipeline_id,
                Version(1),
                Uuid::from_u128(456),
                json!({
                    "inputs": {}
                }),
            )
            .await
            .unwrap();

        // Still not collecting data for this pipelines
        collector.check_for_new_pipelines().await.unwrap();
        assert_eq!(collector.schedule.len(), 0);

        db.lock()
            .await
            .transit_deployment_resources_status_to_provisioned(
                tenant_id,
                pipeline_id,
                Version(1),
                "test-location",
                ExtendedRuntimeStatus {
                    runtime_status: RuntimeStatus::Initializing,
                    runtime_status_details: json!(""),
                    runtime_desired_status: RuntimeDesiredStatus::Running,
                },
            )
            .await
            .unwrap();

        // Still not collecting data for this pipelines
        collector.check_for_new_pipelines().await.unwrap();
        assert_eq!(collector.schedule.len(), 0);

        db.lock()
            .await
            .transit_deployment_resources_status_to_provisioned(
                tenant_id,
                pipeline_id,
                Version(1),
                "test-location",
                ExtendedRuntimeStatus {
                    runtime_status: RuntimeStatus::Running,
                    runtime_status_details: json!(""),
                    runtime_desired_status: RuntimeDesiredStatus::Running,
                },
            )
            .await
            .unwrap();

        // Check for new pipelines - should add the running pipeline
        collector.check_for_new_pipelines().await.unwrap();
        assert_eq!(collector.schedule.len(), 1);
        let scheduled_pipeline_id = collector.schedule.values().next().unwrap().pipeline_id;
        assert_eq!(scheduled_pipeline_id, pipeline_id);

        // Stop the pipeline
        db.lock()
            .await
            .set_deployment_resources_desired_status_stopped(tenant_id, "test_pipeline")
            .await
            .unwrap();

        db.lock()
            .await
            .transit_deployment_resources_status_to_stopping(
                tenant_id,
                pipeline_id,
                Version(1),
                None,
                None,
            )
            .await
            .unwrap();

        db.lock()
            .await
            .transit_deployment_resources_status_to_stopped(tenant_id, pipeline_id, Version(1))
            .await
            .unwrap();

        // Stopped...
        collector.collect_due_pipelines().await.unwrap();
        assert_eq!(collector.schedule.len(), 0);

        // Remains out of schedule...
        collector.check_for_new_pipelines().await.unwrap();
        assert_eq!(collector.schedule.len(), 0);

        // Start the pipeline again
        db.lock()
            .await
            .set_deployment_resources_desired_status_provisioned(
                tenant_id,
                "test_pipeline",
                RuntimeDesiredStatus::Running,
                BootstrapPolicy::default(),
            )
            .await
            .unwrap();
        db.lock()
            .await
            .transit_deployment_resources_status_to_provisioning(
                tenant_id,
                pipeline_id,
                Version(1),
                Uuid::from_u128(123),
                json!({
                    "inputs": {}
                }),
            )
            .await
            .unwrap();
        db.lock()
            .await
            .transit_deployment_resources_status_to_provisioned(
                tenant_id,
                pipeline_id,
                Version(1),
                "test-location",
                ExtendedRuntimeStatus {
                    runtime_status: RuntimeStatus::Initializing,
                    runtime_status_details: json!(""),
                    runtime_desired_status: RuntimeDesiredStatus::Running,
                },
            )
            .await
            .unwrap();
        db.lock()
            .await
            .transit_deployment_resources_status_to_provisioned(
                tenant_id,
                pipeline_id,
                Version(1),
                "test-location",
                ExtendedRuntimeStatus {
                    runtime_status: RuntimeStatus::Running,
                    runtime_status_details: json!(""),
                    runtime_desired_status: RuntimeDesiredStatus::Running,
                },
            )
            .await
            .unwrap();

        // Check for new pipelines - should add the running pipeline again
        collector.check_for_new_pipelines().await.unwrap();
        assert_eq!(collector.schedule.len(), 1);
        let scheduled_pipeline_id = collector.schedule.values().next().unwrap().pipeline_id;
        assert_eq!(scheduled_pipeline_id, pipeline_id);

        // Signal shutdown
        let _ = shutdown_tx.send(true);
    }
}
