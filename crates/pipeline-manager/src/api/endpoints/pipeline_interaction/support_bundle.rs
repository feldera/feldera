//! Endpoint to retrieve a support bundles for pipeline.
use actix_web::rt::time::timeout;
use chrono::{DateTime, Utc};
use feldera_types::error::ErrorResponse;
use futures_util::StreamExt;

use std::io::Write;
use std::time::Duration;
use zip::{CompressionMethod, ZipWriter};

use crate::api::endpoints::config::Configuration;
use crate::api::error::ApiError;
use crate::api::examples;
use crate::api::main::ServerState;
use crate::db::types::pipeline::ExtendedPipelineDescr;
use crate::db::{storage::Storage, types::tenant::TenantId};
use crate::error::ManagerError;
use actix_web::{
    get,
    http::Method,
    web::{self, Data as WebData, ReqData},
    HttpResponse,
};

type BundleResult<T> = Result<T, String>;

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
    let next_line_timeout = Duration::from_millis(250);
    let mut logs = String::with_capacity(4096);

    let response = state
        .runner
        .get_logs_from_pipeline(client, tenant_id, pipeline_name)
        .await?;

    let mut response = response;
    while let Ok(Some(chunk)) = timeout(
        if first_line {
            Duration::from_secs(5)
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

/// Convert and augment the HTTP response into a bundle result which
/// has string based error reporting for embedding errors within the bundle.
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
#[derive(Debug)]
pub struct SupportBundleData {
    pub time: DateTime<Utc>,
    pub circuit_profile: BundleResult<Vec<u8>>,
    pub heap_profile: BundleResult<Vec<u8>>,
    pub metrics: BundleResult<Vec<u8>>,
    pub logs: BundleResult<String>,
    pub stats: BundleResult<Vec<u8>>,
    pub pipeline_config: BundleResult<Vec<u8>>,
    pub system_config: BundleResult<Vec<u8>>,
}

impl SupportBundleData {
    pub async fn collect(
        state: &WebData<ServerState>,
        client: &awc::Client,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<Self, ManagerError> {
        // Launch all primary data collection tasks in parallel
        let (
            circuit_profile_result,
            heap_profile_result,
            metrics_result,
            logs_result,
            stats_result,
            pipeline_config_result,
            system_config_result,
        ) = tokio::join!(
            fetch_pipeline_data(
                state,
                client,
                tenant_id,
                pipeline_name,
                "dump_profile",
                Some(Duration::from_secs(120))
            ),
            fetch_pipeline_data(
                state,
                client,
                tenant_id,
                pipeline_name,
                "heap_profile",
                None
            ),
            fetch_pipeline_data(state, client, tenant_id, pipeline_name, "metrics", None),
            collect_pipeline_logs(state, client, tenant_id, pipeline_name),
            fetch_pipeline_data(state, client, tenant_id, pipeline_name, "stats", None),
            Self::get_pipeline_configuration(state, tenant_id, pipeline_name),
            Self::get_system_configuration(state),
        );

        // Process results and extract data
        let circuit_profile = response_to_bundle_result(circuit_profile_result).await;
        let heap_profile = response_to_bundle_result(heap_profile_result).await;
        let metrics = response_to_bundle_result(metrics_result).await;
        let logs = logs_result.map_err(|e| e.to_string());
        let stats = response_to_bundle_result(stats_result)
            .await
            .map(json_prettify);
        let pipeline_config = pipeline_config_result.map_err(|e| e.to_string());
        let system_config = system_config_result.map_err(|e| e.to_string());

        Ok(SupportBundleData {
            time: chrono::Utc::now(),
            circuit_profile,
            heap_profile,
            metrics,
            logs,
            stats,
            pipeline_config,
            system_config,
        })
    }

    #[allow(clippy::type_complexity)]
    async fn push_to_zip(
        &self,
        add_to_zip: &mut dyn FnMut(&str, &[u8]) -> Result<(), Box<dyn std::error::Error>>,
    ) -> Result<(Vec<String>, Vec<String>), Box<dyn std::error::Error>> {
        let mut manifest_entries = Vec::new();
        let mut error_entries = Vec::new();

        // Add circuit profile
        match &self.circuit_profile {
            Ok(content) => {
                let _ = add_to_zip("circuit_profile.zip", content);
                manifest_entries.push("✓ circuit_profile.zip".to_string());
            }
            Err(e) => {
                error_entries.push(format!("✗ circuit_profile.zip: {}", e));
            }
        }

        // Add heap profile
        match &self.heap_profile {
            Ok(content) => {
                let _ = add_to_zip("heap_profile.pb.gz", content);
                manifest_entries.push("✓ heap_profile.pb.gz".to_string());
            }
            Err(e) => {
                error_entries.push(format!("✗ heap_profile.pb.gz: {}", e));
            }
        }

        // Add metrics
        match &self.metrics {
            Ok(content) => {
                let _ = add_to_zip("metrics.txt", content);
                manifest_entries.push("✓ metrics.txt".to_string());
            }
            Err(e) => {
                error_entries.push(format!("✗ metrics.txt: {}", e));
            }
        }

        // Add logs
        match &self.logs {
            Ok(content) => {
                let _ = add_to_zip("logs.txt", content.as_bytes());
                manifest_entries.push("✓ logs.txt".to_string());
            }
            Err(e) => {
                error_entries.push(format!("✗ logs.txt: {}", e));
            }
        }

        // Add stats
        match &self.stats {
            Ok(content) => {
                let _ = add_to_zip("stats.json", content);
                manifest_entries.push("✓ stats.json".to_string());
            }
            Err(e) => {
                error_entries.push(format!("✗ stats.json: {}", e));
            }
        }

        // Add pipeline config
        match &self.pipeline_config {
            Ok(content) => {
                let _ = add_to_zip("pipeline_config.json", content);
                manifest_entries.push("✓ pipeline_config.json".to_string());
            }
            Err(e) => {
                error_entries.push(format!("✗ pipeline_config.json: {}", e));
            }
        }

        // Add system config
        match &self.system_config {
            Ok(content) => {
                let _ = add_to_zip("system_config.json", content);
                manifest_entries.push("✓ system_config.json".to_string());
            }
            Err(e) => {
                error_entries.push(format!("✗ system_config.json: {}", e));
            }
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
    async fn get_system_configuration(
        state: &WebData<ServerState>,
    ) -> Result<Vec<u8>, ManagerError> {
        let config = Configuration::gather(state).await;
        serde_json::to_vec_pretty(&config).map_err(|e| {
            ManagerError::from(ApiError::UnableToCreateSupportBundle {
                reason: format!("Failed to serialize system config: {}", e),
            })
        })
    }
}

/// A collection of support bundle data for a single pipeline gathered in a single ZIP file.
struct SupportBundleZip {
    buffer: Vec<u8>,
}

impl SupportBundleZip {
    /// Create a ZIP file from support bundle data.
    async fn create(
        pipeline: &ExtendedPipelineDescr,
        bundles: Vec<SupportBundleData>,
    ) -> Result<SupportBundleZip, ManagerError> {
        let mut zip_buffer = Vec::with_capacity(256 * 1024);
        let mut zip = ZipWriter::new(std::io::Cursor::new(&mut zip_buffer));
        let options =
            zip::write::FileOptions::default().compression_method(CompressionMethod::Deflated);
        let mut manifest = String::with_capacity(8 * 1024);
        manifest.push_str(&format!(
            "\n# Support Bundle Table of Contents\n\nRequested at: {}\nPipeline Status: {} (since {})\n",
            chrono::Utc::now().to_rfc3339(),
            pipeline.deployment_status,
            pipeline.deployment_status_since.to_rfc3339(),
        ));
        if pipeline.deployment_status.to_string() != pipeline.deployment_desired_status.to_string()
        {
            manifest.push_str(&format!(
                "Pipeline Desired Status: {}\n",
                pipeline.deployment_desired_status
            ));
        }

        for (idx, data) in bundles.iter().enumerate() {
            let timestamp = data.time.to_rfc3339();
            manifest.push_str(&format!("\n## Collection {idx} ({})\n", &timestamp));
            let mut add_to_zip =
                |filename: &str, content: &[u8]| -> Result<(), Box<dyn std::error::Error>> {
                    let timestamped_filename = format!("{}_{}", timestamp, filename);
                    zip.start_file(&timestamped_filename, options)?;
                    zip.write_all(content)?;
                    Ok(())
                };

            let (manifest_entries, error_entries) =
                data.push_to_zip(&mut add_to_zip).await.map_err(|e| {
                    ManagerError::from(ApiError::UnableToCreateSupportBundle {
                        reason: format!("Failed to add data to zip: {}", e),
                    })
                })?;

            if !manifest_entries.is_empty() {
                manifest.push_str("\nSuccessfully Collected:\n");
                for entry in manifest_entries {
                    manifest.push_str(&format!("  {}\n", entry));
                }
            }

            if !error_entries.is_empty() {
                manifest.push_str("\nFailed To Collect:\n");
                for entry in error_entries {
                    manifest.push_str(&format!("  {}\n", entry));
                }
            }
        }
        zip.start_file("manifest.txt", options).map_err(|e| {
            ManagerError::from(ApiError::UnableToCreateSupportBundle {
                reason: format!("Failed to add manifest file into bundle: {}", e),
            })
        })?;
        zip.write_all(&manifest.into_bytes()).map_err(|e| {
            ManagerError::from(ApiError::UnableToCreateSupportBundle {
                reason: format!("Failed to write manifest file into bundle: {}", e),
            })
        })?;

        let _r: std::io::Cursor<&mut Vec<u8>> = zip.finish().map_err(|e| {
            ManagerError::from(ApiError::UnableToCreateSupportBundle {
                reason: format!("Failed to create support bundle: {}", e),
            })
        })?;
        drop(zip);

        Ok(SupportBundleZip { buffer: zip_buffer })
    }
}

/// Generate a support bundle containing diagnostic information from a pipeline.
///
/// This endpoint collects various diagnostic data from the pipeline including
/// circuit profile, heap profile, metrics, logs, stats, and connector statistics,
/// and packages them into a single ZIP file for support purposes.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    responses(
        (status = OK
            , description = "Support bundle containing diagnostic information"
            , content_type = "application/zip"
            , body = Vec<u8>),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline_name())),
        (status = SERVICE_UNAVAILABLE
            , body = ErrorResponse
            , examples(
                ("Pipeline is not deployed" = (value = json!(examples::error_pipeline_interaction_not_deployed()))),
                ("Pipeline is currently unavailable" = (value = json!(examples::error_pipeline_interaction_currently_unavailable()))),
                ("Disconnected during response" = (value = json!(examples::error_pipeline_interaction_disconnected()))),
                ("Response timeout" = (value = json!(examples::error_pipeline_interaction_timeout())))
            )
        ),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Pipeline interaction"
)]
#[get("/pipelines/{pipeline_name}/support_bundle")]
pub(crate) async fn get_pipeline_support_bundle(
    state: WebData<ServerState>,
    client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    let pipeline = state
        .db
        .lock()
        .await
        .get_pipeline(*tenant_id, &pipeline_name)
        .await?;
    let data = SupportBundleData::collect(&state, &client, *tenant_id, &pipeline_name).await?;
    let bundle = SupportBundleZip::create(&pipeline, vec![data]).await?;

    Ok(HttpResponse::Ok()
        .content_type("application/zip")
        .insert_header((
            "Content-Disposition",
            format!(
                "attachment; filename=\"{}-support-bundle.zip\"",
                pipeline_name
            ),
        ))
        .body(bundle.buffer))
}
