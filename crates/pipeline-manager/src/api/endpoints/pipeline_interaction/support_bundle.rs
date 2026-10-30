//! Endpoint to retrieve a support bundles for pipeline.

use std::io::Write;
use zip::{CompressionMethod, ZipWriter};

use crate::api::error::ApiError;
use crate::api::examples;
use crate::api::main::ServerState;
use crate::api::support_data_collector::{
    CollectionSummary, SupportBundleData, SupportBundleParameters,
};
use crate::db::types::combined_status::{CombinedDesiredStatus, CombinedStatus, combine_since};
use crate::db::types::pipeline::ExtendedPipelineDescrMonitoring;
use crate::db::{storage::Storage, types::tenant::TenantId};
use crate::error::ManagerError;
use actix_web::{
    HttpResponse, get,
    web::{self, Data as WebData, ReqData},
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashSet;

/// Top-level bundle metadata serialized into both `metadata.txt` (human form)
/// and `metadata.json` (machine form) at the ZIP root.
#[derive(Debug, Serialize)]
struct BundleMetadata<'a> {
    /// Wire format version of the metadata document itself.
    metadata_version: u32,
    requested_at: DateTime<Utc>,
    pipeline: PipelineMetadata<'a>,
    collections: Vec<CollectionMetadata>,
}

#[derive(Debug, Serialize)]
struct PipelineMetadata<'a> {
    name: &'a str,
    status: String,
    status_since: DateTime<Utc>,
    /// Present only when the desired status differs from the current status.
    #[serde(skip_serializing_if = "Option::is_none")]
    desired_status: Option<String>,
}

#[derive(Debug, Serialize)]
struct CollectionMetadata {
    /// Directory inside the ZIP that holds this collection's files.
    directory: String,
    time: DateTime<Utc>,
    /// Version of the support bundle data format that produced this entry.
    data_version: u32,
    #[serde(flatten)]
    summary: CollectionSummary,
}

const METADATA_VERSION: u32 = 1;

/// Format `DateTime<Utc>` for use as a directory name. Colons are problematic
/// on some filesystems and inside ZIP tooling, so we use a hyphen-separated
/// layout (e.g., `2026-05-25T12-34-56.789Z`).
fn timestamp_dir(time: &DateTime<Utc>) -> String {
    time.format("%Y-%m-%dT%H-%M-%S%.fZ").to_string()
}

/// Reserve a unique directory name inside the ZIP. If `candidate` was already
/// used (extremely unlikely: would require two stored collections sharing the
/// nanosecond-resolution `time`), append `-1`, `-2`, ... until free. Mutates
/// the set so subsequent calls see the reservation.
fn unique_directory(candidate: &str, used: &mut HashSet<String>) -> String {
    if used.insert(candidate.to_string()) {
        return candidate.to_string();
    }
    let mut suffix = 1u32;
    loop {
        let attempt = format!("{candidate}-{suffix}");
        if used.insert(attempt.clone()) {
            return attempt;
        }
        suffix += 1;
    }
}

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
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(CompressionMethod::Deflated);
        let options_for = |method: CompressionMethod| options.compression_method(method);

        let combined_status = CombinedStatus::new(
            pipeline.deployment_resources_status,
            pipeline.deployment_runtime_status,
        );
        let status_since = combine_since(
            pipeline.deployment_resources_status_since,
            pipeline.deployment_runtime_status_since,
        );
        let combined_desired_status = CombinedDesiredStatus::new(
            pipeline.deployment_resources_desired_status,
            pipeline.deployment_initial,
            pipeline.deployment_runtime_desired_status,
        );
        let desired_status =
            if format!("{:?}", combined_status) != format!("{:?}", combined_desired_status) {
                Some(format!("{:?}", combined_desired_status))
            } else {
                None
            };

        let mut collection_meta = Vec::with_capacity(bundles.len());
        let mut used_dirs: HashSet<String> = HashSet::with_capacity(bundles.len());

        // Collections stored before the secret fix are exported as stored.
        let bundles: Vec<SupportBundleData> = bundles
            .into_iter()
            .map(|data| {
                if data.version >= SupportBundleData::REDACTED_SINCE_VERSION {
                    data
                } else {
                    data.redact_diagnostics()
                }
            })
            .collect();
        for data in bundles.iter() {
            let directory = unique_directory(&timestamp_dir(&data.time), &mut used_dirs);
            let dir_prefix = directory.clone();
            // Already-compressed items (gzip, zip) are stored verbatim
            let mut add_to_zip = |filename: &str,
                                  content: &[u8],
                                  method: CompressionMethod|
             -> Result<(), Box<dyn std::error::Error>> {
                let path = format!("{}/{}", dir_prefix, filename);
                zip.start_file(&path, options_for(method))?;
                zip.write_all(content)?;
                Ok(())
            };

            let summary = data
                .push_to_zip(&mut add_to_zip, params)
                .await
                .map_err(|e| {
                    ManagerError::from(ApiError::UnableToCreateSupportBundle {
                        reason: format!("Failed to add data to zip: {}", e),
                    })
                })?;

            collection_meta.push(CollectionMetadata {
                directory,
                time: data.time,
                data_version: data.version,
                summary,
            });
        }

        let metadata = BundleMetadata {
            metadata_version: METADATA_VERSION,
            requested_at: Utc::now(),
            pipeline: PipelineMetadata {
                name: &pipeline.name,
                status: format!("{:?}", combined_status),
                status_since,
                desired_status,
            },
            collections: collection_meta,
        };

        let manifest_text = render_metadata_text(&metadata);
        let manifest_json = serde_json::to_vec_pretty(&metadata).map_err(|e| {
            ManagerError::from(ApiError::UnableToCreateSupportBundle {
                reason: format!("Failed to serialize metadata.json: {}", e),
            })
        })?;

        zip.start_file("metadata.txt", options).map_err(|e| {
            ManagerError::from(ApiError::UnableToCreateSupportBundle {
                reason: format!("Failed to add metadata.txt to bundle: {}", e),
            })
        })?;
        zip.write_all(manifest_text.as_bytes()).map_err(|e| {
            ManagerError::from(ApiError::UnableToCreateSupportBundle {
                reason: format!("Failed to write metadata.txt to bundle: {}", e),
            })
        })?;

        zip.start_file("metadata.json", options).map_err(|e| {
            ManagerError::from(ApiError::UnableToCreateSupportBundle {
                reason: format!("Failed to add metadata.json to bundle: {}", e),
            })
        })?;
        zip.write_all(&manifest_json).map_err(|e| {
            ManagerError::from(ApiError::UnableToCreateSupportBundle {
                reason: format!("Failed to write metadata.json to bundle: {}", e),
            })
        })?;

        let _r: std::io::Cursor<&mut Vec<u8>> = zip.finish().map_err(|e| {
            ManagerError::from(ApiError::UnableToCreateSupportBundle {
                reason: format!("Failed to create support bundle: {}", e),
            })
        })?;

        Ok(SupportBundleZip { buffer: zip_buffer })
    }
}

fn render_metadata_text(meta: &BundleMetadata<'_>) -> String {
    let mut out = String::with_capacity(8 * 1024);
    out.push_str("\n# Support Bundle Table of Contents\n\n");
    out.push_str(&format!("Pipeline: {}\n", meta.pipeline.name));
    out.push_str(&format!(
        "Requested at: {}\n",
        meta.requested_at.to_rfc3339()
    ));
    out.push_str(&format!(
        "Pipeline Status: {} (since {})\n",
        meta.pipeline.status,
        meta.pipeline.status_since.to_rfc3339()
    ));
    if let Some(desired) = &meta.pipeline.desired_status {
        out.push_str(&format!("Pipeline Desired Status: {}\n", desired));
    }

    for (idx, collection) in meta.collections.iter().enumerate() {
        out.push_str(&format!(
            "\n## Collection {idx} ({})\nDirectory: {}/\n",
            collection.time.to_rfc3339(),
            collection.directory
        ));

        if !collection.summary.collected.is_empty() {
            out.push_str("\nSuccessfully Collected:\n");
            for entry in &collection.summary.collected {
                out.push_str(&format!("  ✓ {}\n", entry));
            }
        }
        if !collection.summary.failed.is_empty() {
            out.push_str("\nFailed To Collect:\n");
            for entry in &collection.summary.failed {
                out.push_str(&format!("  ✗ {}: {}\n", entry.file, entry.reason));
            }
        }
        if !collection.summary.skipped.is_empty() {
            out.push_str("\nSkipped (by request):\n");
            for entry in &collection.summary.skipped {
                out.push_str(&format!("  ⚠ {}\n", entry));
            }
        }
    }
    out
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
        (status = BAD_REQUEST
            , description = "A query parameter was invalid (e.g., `limit=0`)"
            , body = ErrorResponse),
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
    if support_bundle_params.limit == Some(0) {
        return Err(ManagerError::from(ApiError::InvalidSupportBundleParameter {
            reason: "limit must be at least 1 when provided; omit it to return all retained collections".to_string(),
        }));
    }
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

    // Only collect all requested data from the running pipeline if collect=true
    if support_bundle_params.collect {
        bundles.insert(
            0,
            SupportBundleData::collect(
                &state,
                &client,
                *tenant_id,
                &pipeline_name,
                &support_bundle_params,
            )
            .await?,
        );
    };

    // Keep only the most recent `limit` collections when set. Bundles are
    // already ordered newest-first.
    if let Some(limit) = support_bundle_params.limit {
        bundles.truncate(limit as usize);
    }

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
        .insert_header(actix_http::ContentEncoding::Identity)
        .body(bundle.buffer))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::types::pipeline::PipelineId;
    use crate::db::types::program::ProgramStatus;
    use crate::db::types::resources_status::{ResourcesDesiredStatus, ResourcesStatus};
    use crate::db::types::storage::StorageStatus;
    use crate::db::types::version::Version;
    use std::collections::BTreeSet;
    use std::io::Read;
    use uuid::Uuid;
    use zip::ZipArchive;

    /// Build a minimal monitoring descriptor for tests.
    fn fake_pipeline(name: &str) -> ExtendedPipelineDescrMonitoring {
        ExtendedPipelineDescrMonitoring {
            id: PipelineId(Uuid::from_u128(1)),
            name: name.to_string(),
            description: Default::default(),
            tags: Default::default(),
            created_at: Default::default(),
            version: Version(1),
            platform_version: String::new(),
            runtime_config: Default::default(),
            program_config: Default::default(),
            program_version: Version(1),
            program_status: ProgramStatus::Pending,
            program_status_since: Default::default(),
            deployment_error: None,
            deployment_config: None,
            deployment_location: None,
            refresh_version: Version(1),
            storage_status: StorageStatus::Cleared,
            storage_status_details: None,
            deployment_id: None,
            deployment_initial: None,
            deployment_resources_status: ResourcesStatus::Stopped,
            deployment_resources_status_since: Default::default(),
            deployment_resources_desired_status: ResourcesDesiredStatus::Stopped,
            deployment_resources_desired_status_since: Default::default(),
            deployment_runtime_status: None,
            deployment_runtime_status_details: None,
            deployment_runtime_status_since: None,
            deployment_runtime_desired_status: None,
            bootstrap_policy: None,
            deployment_runtime_desired_status_since: None,
        }
    }

    fn read_zip(buffer: &[u8]) -> ZipArchive<std::io::Cursor<&[u8]>> {
        ZipArchive::new(std::io::Cursor::new(buffer)).unwrap()
    }

    #[tokio::test]
    async fn bundle_layout_groups_files_by_timestamp_directory() {
        let descr = fake_pipeline("p1");
        let params = SupportBundleParameters::default();
        let bundles = vec![
            SupportBundleData::test_data(),
            SupportBundleData::test_data(),
        ];

        let bundle = SupportBundleZip::create(&descr, bundles, &params)
            .await
            .unwrap();
        let mut archive = read_zip(&bundle.buffer);

        let names: Vec<String> = (0..archive.len())
            .map(|i| archive.by_index(i).unwrap().name().unwrap().to_string())
            .collect();

        // `metadata.txt` and `metadata.json` sit at the root.
        assert!(names.iter().any(|n| n == "metadata.txt"));
        assert!(names.iter().any(|n| n == "metadata.json"));

        // Every other entry is `<timestamp>/<file>`.
        let dirs: BTreeSet<&str> = names
            .iter()
            .filter(|n| n.contains('/'))
            .filter_map(|n| n.split('/').next())
            .collect();
        assert_eq!(
            dirs.len(),
            2,
            "expected two collection directories, got {:?}",
            dirs
        );
        for dir in &dirs {
            assert!(
                names
                    .iter()
                    .any(|n| n == &format!("{dir}/circuit_profile.zip")),
                "missing circuit_profile.zip in {dir}"
            );
            // The JSON profile is stored gzip-compressed, exactly as collected.
            let json_gz = archive
                .by_name(&format!("{dir}/circuit_profile.json.gz"))
                .expect("missing circuit_profile.json.gz");
            assert_eq!(json_gz.compression(), CompressionMethod::Stored);
            assert_eq!(
                json_gz.size(),
                3,
                "stored bytes must be the collected bytes"
            );
            assert!(
                names
                    .iter()
                    .any(|n| n == &format!("{dir}/pipeline_events.json")),
                "missing pipeline_events.json in {dir}"
            );
        }

        // Metadata JSON parses and references each directory.
        let mut metadata_json = String::new();
        archive
            .by_name("metadata.json")
            .unwrap()
            .read_to_string(&mut metadata_json)
            .unwrap();
        let meta: serde_json::Value = serde_json::from_str(&metadata_json).unwrap();
        assert_eq!(meta["pipeline"]["name"], "p1");
        let collections = meta["collections"].as_array().unwrap();
        assert_eq!(collections.len(), 2);
        for c in collections {
            assert!(dirs.contains(c["directory"].as_str().unwrap()));
            assert!(
                c["collected"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|f| f == "pipeline_events.json")
            );
        }
    }

    #[tokio::test]
    async fn create_with_no_bundles_still_emits_metadata() {
        let descr = fake_pipeline("empty");
        let params = SupportBundleParameters::default();
        let bundle = SupportBundleZip::create(&descr, vec![], &params)
            .await
            .unwrap();
        let mut archive = read_zip(&bundle.buffer);
        let names: Vec<String> = (0..archive.len())
            .map(|i| archive.by_index(i).unwrap().name().unwrap().to_string())
            .collect();
        assert_eq!(names.iter().filter(|n| n.contains('/')).count(), 0);
        assert!(names.iter().any(|n| n == "metadata.txt"));
        assert!(names.iter().any(|n| n == "metadata.json"));
    }

    #[tokio::test]
    async fn duplicate_collection_timestamps_get_suffixed_directories() {
        let descr = fake_pipeline("p1");
        let params = SupportBundleParameters::default();
        let shared_time = chrono::Utc::now();
        let mut a = SupportBundleData::test_data();
        let mut b = SupportBundleData::test_data();
        a.time = shared_time;
        b.time = shared_time;

        let bundle = SupportBundleZip::create(&descr, vec![a, b], &params)
            .await
            .unwrap();
        let mut archive = read_zip(&bundle.buffer);
        let names: Vec<String> = (0..archive.len())
            .map(|i| archive.by_index(i).unwrap().name().unwrap().to_string())
            .collect();
        let dirs: BTreeSet<&str> = names
            .iter()
            .filter(|n| n.contains('/'))
            .filter_map(|n| n.split('/').next())
            .collect();
        assert_eq!(dirs.len(), 2);
        let base = timestamp_dir(&shared_time);
        assert!(dirs.contains(base.as_str()));
        assert!(dirs.contains(format!("{base}-1").as_str()));
    }

    #[test]
    fn unique_directory_appends_suffix_on_collision() {
        let mut used = HashSet::new();
        assert_eq!(unique_directory("x", &mut used), "x");
        assert_eq!(unique_directory("x", &mut used), "x-1");
        assert_eq!(unique_directory("x", &mut used), "x-2");
        assert_eq!(unique_directory("y", &mut used), "y");
    }

    #[tokio::test]
    async fn metadata_text_lists_pipeline_name_and_collections() {
        let descr = fake_pipeline("supportbundle-test");
        let params = SupportBundleParameters::default();
        let bundle =
            SupportBundleZip::create(&descr, vec![SupportBundleData::test_data()], &params)
                .await
                .unwrap();
        let mut archive = read_zip(&bundle.buffer);
        let mut text = String::new();
        archive
            .by_name("metadata.txt")
            .unwrap()
            .read_to_string(&mut text)
            .unwrap();
        assert!(text.contains("Pipeline: supportbundle-test"));
        assert!(text.contains("Collection 0"));
        assert!(text.contains("Directory:"));
    }

    /// A collection stored before the secret fix holds the resolved Postgres
    /// URI in the record's error, the events, the stats, and the logs. The
    /// exported files must show the URI without its password, while the SQL
    /// is exported as supplied.
    #[tokio::test]
    async fn stored_collections_are_exported_without_url_passwords() {
        let uri = "postgres://feldera:export-secret-2136@db.internal:5432/app";
        let error = format!("Failed to connect to {uri}");
        let record = serde_json::json!({
            "name": "p1",
            "program_code": "CREATE TABLE t (uri VARCHAR) WITH ('connectors' = '[{\"uri\": \"postgres://u:inline-2136@h/db\"}]');",
            "deployment_error": {"message": error, "error_code": "InitializationError", "details": {"error": error}},
        });
        let mut data = SupportBundleData::test_data();
        data.version = 4;
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(&serde_json::to_vec(&record).unwrap()).unwrap();
        data.pipeline_config = Ok(gz.finish().unwrap());
        data.pipeline_events = Ok(serde_json::to_vec(
            &serde_json::json!([{"deployment_error": {"message": error}}]),
        )
        .unwrap());
        data.stats = Ok(serde_json::to_vec(
            &serde_json::json!({"inputs": [{"endpoint_name": "pg_in", "errors": [{"message": error}]}]}),
        )
        .unwrap());
        data.logs = Ok(format!("ERROR pg_in: {error}\n"));

        let bundle = SupportBundleZip::create(
            &fake_pipeline("p1"),
            vec![data],
            &SupportBundleParameters::default(),
        )
        .await
        .unwrap();
        let mut archive = read_zip(&bundle.buffer);
        let names: Vec<String> = (0..archive.len())
            .map(|i| archive.by_index(i).unwrap().name().unwrap().to_string())
            .collect();
        for file in [
            "pipeline_config.json",
            "pipeline_events.json",
            "stats.json",
            "logs.txt",
        ] {
            let name = names.iter().find(|n| n.ends_with(file)).expect(file);
            let mut text = String::new();
            archive
                .by_name(name)
                .unwrap()
                .read_to_string(&mut text)
                .unwrap();
            assert!(!text.contains("export-secret-2136"), "{file}: {text}");
            assert!(
                text.contains("postgres://feldera:[REDACTED]@db.internal:5432/app"),
                "{file}: {text}"
            );
            if file == "pipeline_config.json" {
                assert!(
                    text.contains("inline-2136"),
                    "SQL must be exported as supplied: {text}"
                );
            }
        }
    }

    /// Failed requests have no file payload: their error goes into both
    /// manifests. Cover stored MessagePack collections and fresh collections.
    #[tokio::test]
    async fn collection_failures_are_redacted_in_both_manifests() {
        for version in [5, 6, SupportBundleData::current_version()] {
            for uri in [
                "postgres://u:metadata-secret-2136@host/db",
                "postgres://u@host/db?password=metadata-secret-2136",
                "host=host password='metadata-secret-2136' dbname=db",
            ] {
                let error =
                    format!("HTTP 500: pg_in: Unable to connect to postgres instance '{uri}'");
                let mut data = SupportBundleData::test_data();
                data.version = version;
                data.circuit_profile = Err(error.clone());
                data.json_circuit_profile = Err(error.clone());
                data.heap_profile = Err(error.clone());
                data.metrics = Err(error.clone());
                data.logs = Err(error.clone());
                data.stats = Err(error.clone());
                data.pipeline_config = Err(error.clone());
                data.system_config = Err(error.clone());
                data.dataflow_graph = Err(error.clone());
                data.pipeline_events = Err(error);
                if version == SupportBundleData::current_version() {
                    data = data.redact_diagnostics();
                    assert!(
                        !serde_json::to_string(&data)
                            .unwrap()
                            .contains("metadata-secret-2136")
                    );
                }
                let stored = rmp_serde::to_vec(&data).unwrap();
                let data = rmp_serde::from_slice(&stored).unwrap();
                let bundle = SupportBundleZip::create(
                    &fake_pipeline("p1"),
                    vec![data],
                    &SupportBundleParameters::default(),
                )
                .await
                .unwrap();
                let mut archive = read_zip(&bundle.buffer);
                for file in ["metadata.json", "metadata.txt"] {
                    let mut text = String::new();
                    archive
                        .by_name(file)
                        .unwrap()
                        .read_to_string(&mut text)
                        .unwrap();
                    assert!(!text.contains("metadata-secret-2136"), "{text}");
                    assert!(text.contains("[REDACTED]"), "{text}");
                    assert!(text.contains("stats.json"), "{text}");
                }
            }
        }
    }
}
