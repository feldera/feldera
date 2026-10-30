//! Endpoint to retrieve a support bundles for pipeline.

use std::fmt::Write as _;
use std::io::{Cursor, Write};
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use zip::write::{SimpleFileOptions, StreamWriter};
use zip::{CompressionMethod, ZipArchive, ZipWriter};

use crate::api::error::ApiError;
use crate::api::examples;
use crate::api::main::ServerState;
use crate::api::support_data_collector::{
    CollectionSummary, GzipMember, SupportBundleData, SupportBundleParameters, ZipContent,
};
use crate::db::types::combined_status::{CombinedDesiredStatus, CombinedStatus, combine_since};
use crate::db::types::pipeline::ExtendedPipelineDescrMonitoring;
use crate::db::{storage::Storage, types::tenant::TenantId};
use crate::error::ManagerError;
use actix_web::web::Bytes;
use actix_web::{
    HttpResponse, get,
    web::{self, Data as WebData, ReqData},
};
use async_stream::try_stream;
use chrono::{DateTime, Utc};
use futures_util::{Stream, TryStreamExt};
use serde::Serialize;
use std::collections::HashSet;
use tokio::time::{Instant, interval_at};
use tracing::error;

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

/// A ZIP archive whose one entry, `name`, holds `member`'s deflate stream, so
/// that `ZipWriter::raw_copy_file` can copy the stream without inflating it.
fn deflated_entry_archive(name: &str, member: &GzipMember<'_>) -> Result<Vec<u8>, String> {
    let name = name.as_bytes();
    let name_length = u16::try_from(name.len()).map_err(|_| "file name too long")?;
    let compressed_size = u32::try_from(member.deflate.len()).map_err(|_| "file too large")?;
    let time = zip::DateTime::default_for_write();
    // The fields that the local and the central header share: version needed,
    // flags, method (deflate), time, date, CRC-32, compressed and uncompressed
    // size, name length, extra field length.
    let mut shared = Vec::with_capacity(26);
    for value in [20u16, 0, 8, time.timepart(), time.datepart()] {
        shared.extend(value.to_le_bytes());
    }
    for value in [member.crc32, compressed_size, member.size] {
        shared.extend(value.to_le_bytes());
    }
    for value in [name_length, 0] {
        shared.extend(value.to_le_bytes());
    }

    let mut archive = Vec::with_capacity(member.deflate.len() + 2 * name.len() + 100);
    archive.extend(0x0403_4b50u32.to_le_bytes());
    archive.extend(&shared);
    archive.extend(name);
    archive.extend(member.deflate);
    let directory_offset = u32::try_from(archive.len()).map_err(|_| "file too large")?;
    archive.extend(0x0201_4b50u32.to_le_bytes());
    // Made by Unix, version 2.0, as the other bundle files.
    archive.extend(((3u16 << 8) | 20).to_le_bytes());
    archive.extend(&shared);
    // Comment length, disk number, internal attributes.
    for value in [0u16, 0, 0] {
        archive.extend(value.to_le_bytes());
    }
    // External attributes (a regular file with mode 0644, as the other bundle
    // files), offset of the local header.
    for value in [0o100644u32 << 16, 0] {
        archive.extend(value.to_le_bytes());
    }
    archive.extend(name);
    let directory_size = archive.len() as u32 - directory_offset;
    archive.extend(0x0605_4b50u32.to_le_bytes());
    // Disk number, disk of the directory, entries on this disk, entries.
    for value in [0u16, 0, 1, 1] {
        archive.extend(value.to_le_bytes());
    }
    for value in [directory_size, directory_offset] {
        archive.extend(value.to_le_bytes());
    }
    // Comment length.
    archive.extend(0u16.to_le_bytes());
    Ok(archive)
}

/// Bytes that the ZIP writer wrote and the response did not send yet.
#[derive(Clone, Default)]
struct Pending(Arc<Mutex<Vec<u8>>>);

impl Pending {
    fn take(&self) -> Bytes {
        Bytes::from(std::mem::take(&mut *self.0.lock().unwrap()))
    }
}

impl Write for Pending {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn bundle_error(what: &str, error: impl std::fmt::Display) -> ManagerError {
    ManagerError::from(ApiError::UnableToCreateSupportBundle {
        reason: format!("{what}: {error}"),
    })
}

/// Writes a support bundle ZIP file, one entry after another, so the bytes
/// can be sent while the rest is not written yet.
struct SupportBundleZip {
    zip: ZipWriter<StreamWriter<Pending>>,
    pending: Pending,
    used_dirs: HashSet<String>,
    collections: Vec<CollectionMetadata>,
}

impl SupportBundleZip {
    fn new() -> Self {
        let pending = Pending::default();
        Self {
            zip: ZipWriter::new_stream(pending.clone()),
            pending,
            used_dirs: HashSet::new(),
            collections: Vec::new(),
        }
    }

    fn options() -> SimpleFileOptions {
        SimpleFileOptions::default().compression_method(CompressionMethod::Deflated)
    }

    /// Takes the bytes written since the last call.
    fn take(&self) -> Bytes {
        self.pending.take()
    }

    /// Starts `COLLECTION_LOG`, which records the progress of a collection.
    fn start_log(&mut self) -> Result<(), ManagerError> {
        self.zip
            .start_file(COLLECTION_LOG, Self::options())
            .map_err(|e| bundle_error("Failed to add the collection log to bundle", e))
    }

    /// Writes a line to `COLLECTION_LOG`, and flushes the compressor so that
    /// the line is ready to send.
    fn log(&mut self, line: &str) -> Result<(), ManagerError> {
        writeln!(self.zip, "{} {line}", Utc::now().to_rfc3339())
            .and_then(|()| self.zip.flush())
            .map_err(|e| bundle_error("Failed to write the collection log", e))
    }

    /// Adds the entries of one collection, in its own directory.
    async fn add_collection(
        &mut self,
        data: &SupportBundleData,
        params: &SupportBundleParameters,
    ) -> Result<(), ManagerError> {
        let directory = unique_directory(&timestamp_dir(&data.time), &mut self.used_dirs);
        let zip = &mut self.zip;
        let mut add_to_zip =
            |filename: &str, content: ZipContent<'_>| -> Result<(), Box<dyn std::error::Error>> {
                let path = format!("{directory}/{filename}");
                match content {
                    ZipContent::Bytes(bytes, method) => {
                        zip.start_file(&path, Self::options().compression_method(method))?;
                        zip.write_all(bytes)?;
                    }
                    ZipContent::Gzip(member) => {
                        let archive = deflated_entry_archive(&path, &member)?;
                        let mut archive = ZipArchive::new(Cursor::new(archive))?;
                        zip.raw_copy_file(archive.by_index_raw(0)?)?;
                    }
                }
                Ok(())
            };
        let summary = data
            .push_to_zip(&mut add_to_zip, params)
            .await
            .map_err(|e| bundle_error("Failed to add data to zip", e))?;
        self.collections.push(CollectionMetadata {
            directory,
            time: data.time,
            data_version: data.version,
            summary,
        });
        Ok(())
    }

    /// Writes the metadata entries and the ZIP central directory, and
    /// returns the bytes not taken yet.
    fn finish(mut self, pipeline: &ExtendedPipelineDescrMonitoring) -> Result<Bytes, ManagerError> {
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

        let metadata = BundleMetadata {
            metadata_version: METADATA_VERSION,
            requested_at: Utc::now(),
            pipeline: PipelineMetadata {
                name: &pipeline.name,
                status: format!("{:?}", combined_status),
                status_since,
                desired_status,
            },
            collections: std::mem::take(&mut self.collections),
        };

        let manifest_text = render_metadata_text(&metadata);
        let manifest_json = serde_json::to_vec_pretty(&metadata)
            .map_err(|e| bundle_error("Failed to serialize metadata.json", e))?;

        for (name, content) in [
            ("metadata.txt", manifest_text.as_bytes()),
            ("metadata.json", &manifest_json),
        ] {
            self.zip
                .start_file(name, Self::options())
                .map_err(|e| bundle_error(&format!("Failed to add {name} to bundle"), e))?;
            self.zip
                .write_all(content)
                .map_err(|e| bundle_error(&format!("Failed to write {name} to bundle"), e))?;
        }

        self.zip
            .finish()
            .map_err(|e| bundle_error("Failed to create support bundle", e))?;
        Ok(self.pending.take())
    }

    /// Creates the whole ZIP archive from `bundles`.
    #[cfg(test)]
    async fn create(
        pipeline: &ExtendedPipelineDescrMonitoring,
        bundles: Vec<SupportBundleData>,
        params: &SupportBundleParameters,
    ) -> Result<Vec<u8>, ManagerError> {
        let mut zip = Self::new();
        for data in bundles.into_iter().map(exportable) {
            zip.add_collection(&data, params).await?;
        }
        Ok(zip.finish(pipeline)?.to_vec())
    }
}

/// Reapplies the current redaction to a collection stored before
/// [`SupportBundleData::REDACTED_SINCE_VERSION`]: an older scrubber, or none,
/// can have left connection credentials in it. A newer collection was
/// redacted with the current policy when it was collected.
fn exportable(data: SupportBundleData) -> SupportBundleData {
    if data.version >= SupportBundleData::REDACTED_SINCE_VERSION {
        data
    } else {
        data.redact_diagnostics()
    }
}

/// Name of the ZIP entry that records the progress of a new collection.
const COLLECTION_LOG: &str = "collection_log.txt";

/// Time between two lines in `COLLECTION_LOG` while a collection runs. Each
/// line sends bytes, so a proxy does not close the response as idle.
const COLLECTION_LOG_INTERVAL: Duration = Duration::from_secs(10);

/// Streams the support bundle ZIP file.
///
/// If `collection` is set, it is the new collection: the bundle starts with
/// `COLLECTION_LOG`, which gets a line every `interval` until the collection
/// is complete. Then come the new collection, the collections in `stored`
/// (newest first), and the metadata, up to `params.limit` collections.
fn support_bundle_stream<C>(
    pipeline: ExtendedPipelineDescrMonitoring,
    mut stored: Vec<SupportBundleData>,
    collection: Option<C>,
    params: SupportBundleParameters,
    interval: Duration,
) -> impl Stream<Item = Result<Bytes, ManagerError>>
where
    C: Future<Output = Result<SupportBundleData, ManagerError>>,
{
    try_stream! {
        let mut zip = SupportBundleZip::new();
        if let Some(collection) = collection {
            let started = Instant::now();
            zip.start_log()?;
            zip.log("Collecting data from the pipeline.")?;
            yield zip.take();
            let mut collection = pin!(collection);
            let mut ticks = interval_at(started + interval, interval);
            let data = loop {
                tokio::select! {
                    data = &mut collection => break data,
                    _ = ticks.tick() => {}
                }
                zip.log(&format!(
                    "Still collecting after {} s.",
                    started.elapsed().as_secs()
                ))?;
                yield zip.take();
            }?;
            zip.log(&format!(
                "Collected in {} s.",
                started.elapsed().as_secs()
            ))?;
            stored.insert(0, data);
        }

        // Keep only the most recent `limit` collections when set. Bundles are
        // already ordered newest-first.
        if let Some(limit) = params.limit {
            stored.truncate(limit as usize);
        }
        for data in stored.into_iter().map(exportable) {
            zip.add_collection(&data, &params).await?;
            yield zip.take();
        }
        yield zip.finish(&pipeline)?;
    }
}

fn render_metadata_text(meta: &BundleMetadata<'_>) -> String {
    let mut out = String::with_capacity(8 * 1024);
    out.push_str("\n# Support Bundle Table of Contents\n\n");
    let _ = writeln!(&mut out, "Pipeline: {}", meta.pipeline.name);
    let _ = writeln!(&mut out, "Requested at: {}", meta.requested_at.to_rfc3339());
    let _ = writeln!(
        &mut out,
        "Pipeline Status: {} (since {})",
        meta.pipeline.status,
        meta.pipeline.status_since.to_rfc3339()
    );
    if let Some(desired) = &meta.pipeline.desired_status {
        let _ = writeln!(&mut out, "Pipeline Desired Status: {desired}");
    }

    for (idx, collection) in meta.collections.iter().enumerate() {
        let _ = write!(
            &mut out,
            "\n## Collection {idx} ({})\nDirectory: {}/\n",
            collection.time.to_rfc3339(),
            collection.directory
        );

        if !collection.summary.collected.is_empty() {
            out.push_str("\nSuccessfully Collected:\n");
            for entry in &collection.summary.collected {
                let _ = writeln!(&mut out, "  ✓ {entry}");
            }
        }
        if !collection.summary.failed.is_empty() {
            out.push_str("\nFailed To Collect:\n");
            for entry in &collection.summary.failed {
                let _ = writeln!(&mut out, "  ✗ {}: {}", entry.file, entry.reason);
            }
        }
        if !collection.summary.skipped.is_empty() {
            out.push_str("\nSkipped (by request):\n");
            for entry in &collection.summary.skipped {
                let _ = writeln!(&mut out, "  ⚠ {entry}");
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
///
/// The response starts before the data is collected, and has no
/// `Content-Length`. When the bundle collects new data, its first entry,
/// `collection_log.txt`, gets a line every 10 seconds until the collection is
/// complete, so that the response is never idle for long. Data that the
/// pipeline cannot supply is reported inside the bundle. A failure after the
/// response started cuts the response short, so a client must check that the
/// ZIP file is complete.
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
            , description = "The database was busy before the response started (`LockTookTooLong` or `DeadlockDetected`)"
            , body = ErrorResponse),
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
    let (pipeline, bundles) = state
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
    let tenant_id = *tenant_id;
    let collection = support_bundle_params.collect.then_some(async move {
        SupportBundleData::collect(
            &state,
            &client,
            tenant_id,
            &pipeline_name,
            &support_bundle_params,
        )
        .await
    });
    let filename = format!("{}-support-bundle.zip", pipeline.name);
    let body = support_bundle_stream(
        pipeline,
        bundles,
        collection,
        support_bundle_params,
        COLLECTION_LOG_INTERVAL,
    )
    .inspect_err(|e| error!("Support bundle stopped after its response started: {e}"));

    // The response starts before the collection, so the client and any proxy
    // in between see bytes while it runs. A failure after this point cuts the
    // response short instead of setting an error status.
    Ok(HttpResponse::Ok()
        .content_type("application/zip")
        .insert_header((
            "Content-Disposition",
            format!("attachment; filename=\"{filename}\""),
        ))
        .insert_header(actix_http::ContentEncoding::Identity)
        .streaming(body))
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
        let mut archive = read_zip(&bundle);

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

    /// The bundle carries the stored deflate stream of the pipeline config
    /// unchanged, and the file reads back as the original JSON.
    #[tokio::test]
    async fn pipeline_config_is_copied_without_recompressing() {
        let json = serde_json::to_vec_pretty(&serde_json::json!({
            "name": "p",
            "program_code": "SELECT 1;\n".repeat(100),
        }))
        .unwrap();
        // Without compression, so that a recompressed file would be much smaller.
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::none());
        encoder.write_all(&json).unwrap();
        let gz = encoder.finish().unwrap();
        let mut data = SupportBundleData::test_data();
        data.pipeline_config = Ok(gz.clone());

        let bundle = SupportBundleZip::create(
            &fake_pipeline("p"),
            vec![data],
            &SupportBundleParameters::default(),
        )
        .await
        .unwrap();
        let mut archive = read_zip(&bundle);
        let name = archive
            .file_names()
            .map(|name| name.unwrap().to_string())
            .find(|name| name.ends_with("/pipeline_config.json"))
            .unwrap();
        let mut config = archive.by_name(&name).unwrap();
        assert_eq!(config.compression(), CompressionMethod::Deflated);
        assert_eq!(config.unix_mode(), Some(0o100644));
        // The gzip member without its 10-byte header and 8-byte trailer.
        assert_eq!(config.compressed_size(), gz.len() as u64 - 18);
        let mut read = Vec::new();
        // Reading to the end checks the CRC-32.
        config.read_to_end(&mut read).unwrap();
        assert_eq!(read, json);
    }

    #[tokio::test]
    async fn create_with_no_bundles_still_emits_metadata() {
        let descr = fake_pipeline("empty");
        let params = SupportBundleParameters::default();
        let bundle = SupportBundleZip::create(&descr, vec![], &params)
            .await
            .unwrap();
        let mut archive = read_zip(&bundle);
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
        let mut archive = read_zip(&bundle);
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

    /// Every item that `support_bundle_stream` yields for `stored` and
    /// `collection`.
    async fn stream_results<C>(
        stored: Vec<SupportBundleData>,
        collection: Option<C>,
        params: SupportBundleParameters,
    ) -> Vec<Result<Bytes, ManagerError>>
    where
        C: Future<Output = Result<SupportBundleData, ManagerError>>,
    {
        use futures_util::StreamExt;
        support_bundle_stream(
            fake_pipeline("p"),
            stored,
            collection,
            params,
            Duration::from_secs(10),
        )
        .collect()
        .await
    }

    /// The ZIP file made of `results`, which must all be `Ok`.
    fn zip_of(results: Vec<Result<Bytes, ManagerError>>) -> Vec<u8> {
        results
            .into_iter()
            .flat_map(|chunk| chunk.unwrap().to_vec())
            .collect()
    }

    /// The directories of the collections listed in `bundle`'s metadata.
    fn collection_dirs(bundle: &[u8]) -> Vec<String> {
        let mut metadata = String::new();
        read_zip(bundle)
            .by_name("metadata.json")
            .unwrap()
            .read_to_string(&mut metadata)
            .unwrap();
        let metadata: serde_json::Value = serde_json::from_str(&metadata).unwrap();
        metadata["collections"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["directory"].as_str().unwrap().to_string())
            .collect()
    }

    /// `limit` counts the new collection, which comes first.
    #[tokio::test]
    async fn limit_counts_the_new_collection() {
        let mut new = SupportBundleData::test_data();
        new.time += chrono::Duration::days(1);
        let new_dir = timestamp_dir(&new.time);
        let params = SupportBundleParameters {
            limit: Some(1),
            ..Default::default()
        };
        let stored = vec![
            SupportBundleData::test_data(),
            SupportBundleData::test_data(),
        ];
        let results = stream_results(stored, Some(async { Ok(new) }), params).await;
        assert_eq!(collection_dirs(&zip_of(results)), vec![new_dir]);
    }

    /// Without a new collection, the bundle has no collection log.
    #[tokio::test]
    async fn bundle_without_collection_has_no_collection_log() {
        let results = stream_results(
            vec![SupportBundleData::test_data()],
            None::<std::future::Ready<Result<SupportBundleData, ManagerError>>>,
            SupportBundleParameters::default(),
        )
        .await;
        let bundle = zip_of(results);
        assert!(read_zip(&bundle).by_name(COLLECTION_LOG).is_err());
        assert_eq!(collection_dirs(&bundle).len(), 1);
    }

    /// A collection that fails after the response started ends the stream
    /// with the error, which cuts the response short.
    #[tokio::test]
    async fn failed_collection_ends_the_stream_with_an_error() {
        let results = stream_results(
            vec![SupportBundleData::test_data()],
            Some(async { Err(bundle_error("collection failed", "test")) }),
            SupportBundleParameters::default(),
        )
        .await;
        let (last, rest) = results.split_last().unwrap();
        assert!(!rest.is_empty());
        assert!(rest.iter().all(|chunk| chunk.is_ok()));
        assert!(
            last.as_ref()
                .is_err_and(|e| e.to_string().contains("collection failed"))
        );
    }

    /// The bundle sends bytes while a slow collection runs, not only at its
    /// end, and the result is still one valid ZIP file.
    #[tokio::test(start_paused = true)]
    async fn bundle_streams_while_a_collection_runs() {
        use futures_util::StreamExt;

        let interval = Duration::from_secs(10);
        let collection_time = Duration::from_secs(35);
        let collection = async move {
            tokio::time::sleep(collection_time).await;
            Ok(SupportBundleData::test_data())
        };
        let started = Instant::now();
        let mut stream = pin!(support_bundle_stream(
            fake_pipeline("p"),
            vec![SupportBundleData::test_data()],
            Some(collection),
            SupportBundleParameters::default(),
            interval,
        ));
        let mut bundle = Vec::new();
        let mut early_chunks = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            if started.elapsed() < collection_time {
                assert!(!chunk.is_empty());
                early_chunks += 1;
            }
            bundle.extend_from_slice(&chunk);
        }
        // One chunk at the start and one per interval.
        assert_eq!(early_chunks, 4);

        let mut archive = read_zip(&bundle);
        let mut log = String::new();
        archive
            .by_name(COLLECTION_LOG)
            .unwrap()
            .read_to_string(&mut log)
            .unwrap();
        assert_eq!(log.lines().count(), 5, "{log}");
        assert!(log.contains("Still collecting after 30 s."), "{log}");
        assert!(log.contains("Collected in 35 s."), "{log}");
        let mut metadata = String::new();
        archive
            .by_name("metadata.json")
            .unwrap()
            .read_to_string(&mut metadata)
            .unwrap();
        let metadata: serde_json::Value = serde_json::from_str(&metadata).unwrap();
        assert_eq!(metadata["collections"].as_array().unwrap().len(), 2);
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
        let mut archive = read_zip(&bundle);
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
        data.logs = Ok(format!(
            "INFO fetching s3://bucket/data.parquet\nERROR pg_in: {error}\nINFO user alice@example.com logged in\n"
        ));

        let bundle = SupportBundleZip::create(
            &fake_pipeline("p1"),
            vec![data],
            &SupportBundleParameters::default(),
        )
        .await
        .unwrap();
        let mut archive = read_zip(&bundle);
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
            if file == "logs.txt" {
                assert!(
                    text.contains("INFO user alice@example.com logged in"),
                    "the other log lines must stay: {text}"
                );
            }
        }
    }

    /// A collection stored before this version is scrubbed again at export.
    /// Round-trip the actual stored encoding before export with collect=false.
    #[tokio::test]
    async fn older_collections_are_scrubbed_again_on_export() {
        for uri in [
            "postgres://u@host/db?%70assword=legacy-secret-2136",
            "postgres://u:legacy-secret-2136 with space@host/db",
            r"host=host password='head\'legacy-secret-2136' dbname=db",
            "host=host password='head\nlegacy-secret-2136' dbname=db",
        ] {
            let mut data = SupportBundleData::test_data();
            data.version = 5;
            let record = serde_json::json!({
                "program_code": "CREATE TABLE t (s VARCHAR); -- inline-config-stays",
                "deployment_error": {"message": uri, "error_code": "InitializationError"},
            });
            let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            gz.write_all(&serde_json::to_vec(&record).unwrap()).unwrap();
            data.pipeline_config = Ok(gz.finish().unwrap());
            data.stats = Ok(serde_json::to_vec(&serde_json::json!({"message": uri})).unwrap());
            data.pipeline_events =
                Ok(serde_json::to_vec(&serde_json::json!([{"message": uri}])).unwrap());
            data.logs = Ok(uri.to_owned());
            let stored = rmp_serde::to_vec(&data).unwrap();
            let restored = rmp_serde::from_slice(&stored).unwrap();
            let params = SupportBundleParameters {
                collect: false,
                ..Default::default()
            };
            let bundle = SupportBundleZip::create(&fake_pipeline("p1"), vec![restored], &params)
                .await
                .unwrap();
            let mut archive = read_zip(&bundle);
            for index in 0..archive.len() {
                let mut file = archive.by_index(index).unwrap();
                let name = file.name().unwrap().to_string();
                if [
                    "pipeline_config.json",
                    "stats.json",
                    "pipeline_events.json",
                    "logs.txt",
                ]
                .iter()
                .any(|suffix| name.ends_with(suffix))
                {
                    let mut text = String::new();
                    file.read_to_string(&mut text).unwrap();
                    assert!(!text.contains("legacy-secret-2136"), "{name}: {text}");
                    assert!(text.contains("[REDACTED]"), "{name}: {text}");
                    if name.ends_with("pipeline_config.json") {
                        assert!(text.contains("inline-config-stays"));
                        assert!(text.contains("InitializationError"));
                    }
                }
            }
        }
    }

    /// Failed requests have no file payload: their error goes into both
    /// manifests. Cover stored MessagePack collections and fresh collections.
    #[tokio::test]
    async fn collection_failures_are_redacted_in_both_manifests() {
        for version in [4, 5, SupportBundleData::current_version()] {
            for uri in [
                "postgres://u:metadata-secret-2136@host/db",
                "postgres://u@host/db?password=metadata-secret-2136",
                "host=host password='metadata-secret-2136' dbname=db",
                "postgres://u@host/db?%70assword=metadata-secret-2136",
                "postgres://u:metadata-secret-2136 with space@host/db",
                r"host=host password='head\'metadata-secret-2136' dbname=db",
                "host=host password='head\nmetadata-secret-2136' dbname=db",
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
                let mut archive = read_zip(&bundle);
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
