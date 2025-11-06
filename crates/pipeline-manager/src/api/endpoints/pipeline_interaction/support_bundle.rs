//! Endpoint to retrieve a support bundles for pipeline.

use std::io::Write;
use zip::{CompressionMethod, ZipWriter};

use crate::api::error::ApiError;
use crate::api::examples;
use crate::api::main::ServerState;
use crate::api::support_data_collector::{SupportBundleData, SupportBundleParameters};
use crate::db::types::combined_status::{combine_since, CombinedDesiredStatus, CombinedStatus};
use crate::db::types::pipeline::ExtendedPipelineDescrMonitoring;
use crate::db::{storage::Storage, types::tenant::TenantId};
use crate::error::ManagerError;
use actix_web::{
    get,
    web::{self, Data as WebData, ReqData},
    HttpResponse,
};

/// A collection of support bundle data for a single pipeline gathered in a single ZIP file.
struct SupportBundleZip {
    buffer: Vec<u8>,
}

impl SupportBundleZip {
    /// Create a ZIP archive from support bundle data.
    async fn create(
        pipeline: &ExtendedPipelineDescrMonitoring,
        bundles: Vec<SupportBundleData>,
        params: &SupportBundleParameters,
    ) -> Result<SupportBundleZip, ManagerError> {
        let mut zip_buffer = Vec::with_capacity(256 * 1024);
        let mut zip = ZipWriter::new(std::io::Cursor::new(&mut zip_buffer));
        let options =
            zip::write::FileOptions::default().compression_method(CompressionMethod::Deflated);
        let mut manifest = String::with_capacity(8 * 1024);
        let combined_status = CombinedStatus::new(
            pipeline.deployment_resources_status,
            pipeline.deployment_runtime_status,
        );
        let combined_status_since = combine_since(
            pipeline.deployment_resources_status_since,
            pipeline.deployment_runtime_status_since,
        )
        .to_rfc3339();
        let combined_desired_status = CombinedDesiredStatus::new(
            pipeline.deployment_resources_desired_status,
            pipeline.deployment_initial,
            pipeline.deployment_runtime_desired_status,
        );
        manifest.push_str(&format!(
            "\n# Support Bundle Table of Contents\n\nRequested at: {}\nPipeline Status: {:?} (since {})\n",
            chrono::Utc::now().to_rfc3339(),
            combined_status,
            combined_status_since,
        ));
        if format!("{:?}", combined_status) != format!("{:?}", combined_desired_status) {
            manifest.push_str(&format!(
                "Pipeline Desired Status: {:?}\n",
                combined_desired_status
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

            let (manifest_entries, error_entries) = data
                .push_to_zip(&mut add_to_zip, params)
                .await
                .map_err(|e| {
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

/// Download Support Bundle
///
/// Generate a support bundle for a pipeline.
///
/// This endpoint collects various diagnostic data from the pipeline including
/// circuit profile, heap profile, metrics, logs, stats, and connector statistics,
/// and packages them into a single ZIP file for support purposes.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        SupportBundleParameters,
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
                ("Disconnected during response" = (value = json!(examples::error_pipeline_interaction_disconnected()))),
                ("Response timeout" = (value = json!(examples::error_pipeline_interaction_timeout())))
            )
        ),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Metrics & Debugging"
)]
#[get("/pipelines/{pipeline_name}/support_bundle")]
pub(crate) async fn get_pipeline_support_bundle(
    state: WebData<ServerState>,
    client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
    query: web::Query<SupportBundleParameters>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    let support_bundle_params = query.into_inner();
    let (pipeline, mut bundles) = state
        .db
        .lock()
        .await
        .get_support_bundle_data(
            *tenant_id,
            &pipeline_name,
            state.config.support_data_retention,
        )
        .await?;
    bundles.insert(
        0,
        SupportBundleData::collect(&state, &client, *tenant_id, &pipeline_name).await?,
    );
    let bundle = SupportBundleZip::create(&pipeline, bundles, &support_bundle_params).await?;

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
