use crate::api::error::ApiError;
use crate::common_error::CommonError;
use crate::compiler::error::CompilerError;
use crate::compiler::rust_compiler::{
    CLEANUP_INTERVAL, RustCompilationError, RustCompilationResult, cleanup_pipeline_binaries,
    perform_rust_compilation, rust_compiler_task,
};
use crate::compiler::sql_compiler::{
    ProgramValidationRequest, SqlCompilationError, SqlCompilationOutput, decide_stale_jar,
    ephemeral_compilation_dir, jar_cache_dir, perform_sql_compilation, sql_compiler_task,
    validate_program,
};
use crate::compiler::util::{
    CleanupDecision, DiskSpace, cleanup_specific_directories, cleanup_specific_files,
    pipeline_binary_filename, program_info_filename, validate_is_sha256_checksum,
};
use crate::config::{CommonConfig, CompilerConfig};
use crate::db::probe::DbProbe;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::program::ProgramConfig;
use crate::db::types::tenant::TenantId;
use crate::db::types::version::Version;
use crate::error::ManagerError;
use actix_files::NamedFile;
use actix_web::error::PayloadError;
use actix_web::{HttpRequest, HttpResponse, HttpServer, Responder, get, post, web};
use futures_util::{Stream, StreamExt};
use std::net::TcpListener;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio::{fs, io::AsyncWriteExt, spawn, sync::Mutex};
use tracing::{error, info, warn};
use uuid::Uuid;

/// Decodes the URL encoded parameter value as a string.
fn decode_url_encoded_parameter(
    param: &'static str,
    value: Option<&str>,
) -> Result<String, ManagerError> {
    match value {
        None => Err(ManagerError::from(ApiError::MissingUrlEncodedParam {
            param,
        })),
        Some(value) => Ok(value.to_string()),
    }
}

/// Checks if the required compilation artifacts exist for the specified pipeline and version.
/// If `program_info_integrity_checksum` is "none", only the binary existence is checked.
#[get(
    "/artifacts/{pipeline_id}/{program_version}/{source_checksum}/{binary_integrity_checksum}/{program_info_integrity_checksum}"
)]
async fn check_compilation_artifacts(
    config: web::Data<CompilerConfig>,
    req: HttpRequest,
) -> Result<impl Responder, ManagerError> {
    // Retrieve URL encoded parameters
    let path_parameters = req.match_info();
    let pipeline_id =
        decode_url_encoded_parameter("pipeline_id", path_parameters.get("pipeline_id"))?;
    let program_version =
        decode_url_encoded_parameter("program_version", path_parameters.get("program_version"))?;
    let source_checksum =
        decode_url_encoded_parameter("source_checksum", path_parameters.get("source_checksum"))?;
    let binary_integrity_checksum = decode_url_encoded_parameter(
        "binary_integrity_checksum",
        path_parameters.get("binary_integrity_checksum"),
    )?;
    let program_info_integrity_checksum = decode_url_encoded_parameter(
        "program_info_integrity_checksum",
        path_parameters.get("program_info_integrity_checksum"),
    )?;

    // Validate each of them follows expected format
    let pipeline_id =
        PipelineId(
            Uuid::from_str(&pipeline_id).map_err(|e| ApiError::InvalidUuidParam {
                value: pipeline_id.clone(),
                error: e.to_string(),
            })?,
        );
    let program_version =
        Version(
            i64::from_str(&program_version).map_err(|e| ApiError::InvalidVersionParam {
                value: program_version.clone(),
                error: e.to_string(),
            })?,
        );
    validate_is_sha256_checksum(&source_checksum).map_err(|e| {
        ManagerError::from(ApiError::InvalidChecksumParam {
            value: source_checksum.to_string(),
            error: e,
        })
    })?;

    // A Gen-2 program compiles no binary; the caller passes "none" for the
    // binary integrity checksum to check only the program info artifact.
    let binary_expected = binary_integrity_checksum != "none";
    if binary_expected {
        validate_is_sha256_checksum(&binary_integrity_checksum).map_err(|e| {
            ManagerError::from(ApiError::InvalidChecksumParam {
                value: binary_integrity_checksum.to_string(),
                error: e,
            })
        })?;
    }

    // Form file paths
    let binary_exists = if binary_expected {
        config
            .working_dir()
            .join("rust-compilation")
            .join("pipeline-binaries")
            .join(pipeline_binary_filename(
                &pipeline_id,
                program_version,
                &source_checksum,
                &binary_integrity_checksum,
            ))
            .exists()
    } else {
        false
    };

    if program_info_integrity_checksum == "none" {
        let resp = if binary_exists {
            HttpResponse::Ok().finish()
        } else {
            // return binary not found as json body
            HttpResponse::NotFound().json(serde_json::json!({
                "message": "Binary not found",
                "binary_exists": binary_exists,
            }))
        };
        return Ok(resp);
    }

    validate_is_sha256_checksum(&program_info_integrity_checksum).map_err(|e| {
        ManagerError::from(ApiError::InvalidChecksumParam {
            value: program_info_integrity_checksum.to_string(),
            error: e,
        })
    })?;

    let program_info_file_path = config
        .working_dir()
        .join("rust-compilation")
        .join("pipeline-binaries")
        .join(program_info_filename(
            &pipeline_id,
            program_version,
            &source_checksum,
            &program_info_integrity_checksum,
        ));

    // Check artifact existence and return status + headers indicating presence
    let program_info_exists = program_info_file_path.exists();

    // The Gen-2 engine has no binary, so only the program info must be present.
    let artifacts_present = if binary_expected {
        binary_exists && program_info_exists
    } else {
        program_info_exists
    };
    let resp = if artifacts_present {
        HttpResponse::Ok().finish()
    } else {
        // return binary / program info not found as json body
        HttpResponse::NotFound().json(serde_json::json!({
            "message": "Binary or program info not found",
            "binary_exists": binary_exists,
            "program_info_exists": program_info_exists,
        }))
    };

    Ok(resp)
}

/// Retrieves the binary executable.
#[get("/binary/{pipeline_id}/{program_version}/{source_checksum}/{integrity_checksum}")]
async fn get_binary(
    config: web::Data<CompilerConfig>,
    req: HttpRequest,
) -> Result<impl Responder, ManagerError> {
    // Retrieve URL encoded parameters
    let path_parameters = req.match_info();
    let pipeline_id =
        decode_url_encoded_parameter("pipeline_id", path_parameters.get("pipeline_id"))?;
    let program_version =
        decode_url_encoded_parameter("program_version", path_parameters.get("program_version"))?;
    let source_checksum =
        decode_url_encoded_parameter("source_checksum", path_parameters.get("source_checksum"))?;
    let integrity_checksum = decode_url_encoded_parameter(
        "integrity_checksum",
        path_parameters.get("integrity_checksum"),
    )?;

    // Validate each of them follows expected format
    let pipeline_id =
        PipelineId(
            Uuid::from_str(&pipeline_id).map_err(|e| ApiError::InvalidUuidParam {
                value: pipeline_id.clone(),
                error: e.to_string(),
            })?,
        );
    let program_version =
        Version(
            i64::from_str(&program_version).map_err(|e| ApiError::InvalidVersionParam {
                value: program_version.clone(),
                error: e.to_string(),
            })?,
        );
    validate_is_sha256_checksum(&source_checksum).map_err(|e| {
        ManagerError::from(ApiError::InvalidChecksumParam {
            value: source_checksum.to_string(),
            error: e,
        })
    })?;
    validate_is_sha256_checksum(&integrity_checksum).map_err(|e| {
        ManagerError::from(ApiError::InvalidChecksumParam {
            value: integrity_checksum.to_string(),
            error: e,
        })
    })?;

    // Form file path
    let binary_file_path = config
        .working_dir()
        .join("rust-compilation")
        .join("pipeline-binaries")
        .join(pipeline_binary_filename(
            &pipeline_id,
            program_version,
            &source_checksum,
            &integrity_checksum,
        ));

    // Read and return file as response
    Ok(NamedFile::open_async(binary_file_path).await)
}

/// Retrieves the program info file (that contains `PipelineConfigProgramInfo` data).
#[get("/program_info/{pipeline_id}/{program_version}/{source_checksum}/{integrity_checksum}")]
async fn get_program_info(
    config: web::Data<CompilerConfig>,
    req: HttpRequest,
) -> Result<impl Responder, ManagerError> {
    // Retrieve URL encoded parameters
    let path_parameters = req.match_info();
    let pipeline_id =
        decode_url_encoded_parameter("pipeline_id", path_parameters.get("pipeline_id"))?;
    let program_version =
        decode_url_encoded_parameter("program_version", path_parameters.get("program_version"))?;
    let source_checksum =
        decode_url_encoded_parameter("source_checksum", path_parameters.get("source_checksum"))?;
    let integrity_checksum = decode_url_encoded_parameter(
        "integrity_checksum",
        path_parameters.get("integrity_checksum"),
    )?;

    // Validate each of them follows expected format
    let pipeline_id =
        PipelineId(
            Uuid::from_str(&pipeline_id).map_err(|e| ApiError::InvalidUuidParam {
                value: pipeline_id.clone(),
                error: e.to_string(),
            })?,
        );
    let program_version =
        Version(
            i64::from_str(&program_version).map_err(|e| ApiError::InvalidVersionParam {
                value: program_version.clone(),
                error: e.to_string(),
            })?,
        );
    validate_is_sha256_checksum(&source_checksum).map_err(|e| {
        ManagerError::from(ApiError::InvalidChecksumParam {
            value: source_checksum.to_string(),
            error: e,
        })
    })?;
    validate_is_sha256_checksum(&integrity_checksum).map_err(|e| {
        ManagerError::from(ApiError::InvalidChecksumParam {
            value: integrity_checksum.to_string(),
            error: e,
        })
    })?;

    // Form file path
    let info_file_path = config
        .working_dir()
        .join("rust-compilation")
        .join("pipeline-binaries")
        .join(program_info_filename(
            &pipeline_id,
            program_version,
            &source_checksum,
            &integrity_checksum,
        ));

    // Read and return file as response
    Ok(NamedFile::open_async(info_file_path).await)
}

/// Uploads a compiled binary using streaming.
/// Metadata is passed via path parameters and the binary is streamed directly to disk.
#[post("/binary/{pipeline_id}/{program_version}/{source_checksum}/{integrity_checksum}")]
async fn upload_binary(
    config: web::Data<CompilerConfig>,
    req: HttpRequest,
    payload: web::Payload,
) -> Result<impl Responder, ManagerError> {
    // Retrieve URL encoded parameters
    let path_parameters = req.match_info();
    let pipeline_id =
        decode_url_encoded_parameter("pipeline_id", path_parameters.get("pipeline_id"))?;
    let program_version =
        decode_url_encoded_parameter("program_version", path_parameters.get("program_version"))?;
    let source_checksum =
        decode_url_encoded_parameter("source_checksum", path_parameters.get("source_checksum"))?;
    let expected_integrity_checksum = decode_url_encoded_parameter(
        "integrity_checksum",
        path_parameters.get("integrity_checksum"),
    )?;

    // Validate parameters
    let pipeline_id_uuid =
        Uuid::from_str(&pipeline_id).map_err(|e| ApiError::InvalidUuidParam {
            value: pipeline_id.clone(),
            error: e.to_string(),
        })?;
    let pipeline_id = PipelineId(pipeline_id_uuid);

    let program_version =
        Version(
            i64::from_str(&program_version).map_err(|e| ApiError::InvalidVersionParam {
                value: program_version.clone(),
                error: e.to_string(),
            })?,
        );

    validate_is_sha256_checksum(&source_checksum).map_err(|e| {
        ManagerError::from(ApiError::InvalidChecksumParam {
            value: source_checksum.to_string(),
            error: e,
        })
    })?;

    validate_is_sha256_checksum(&expected_integrity_checksum).map_err(|e| {
        ManagerError::from(ApiError::InvalidChecksumParam {
            value: expected_integrity_checksum.to_string(),
            error: e,
        })
    })?;

    // Create pipeline-binaries directory if it doesn't exist
    let pipeline_binaries_dir = config
        .working_dir()
        .join("rust-compilation")
        .join("pipeline-binaries");

    fs::create_dir_all(&pipeline_binaries_dir)
        .await
        .map_err(|e| {
            ManagerError::from(CommonError::io_error(
                format!("creating directory '{}'", pipeline_binaries_dir.display()),
                e,
            ))
        })?;

    // Form the target file path
    let target_file_path = pipeline_binaries_dir.join(pipeline_binary_filename(
        &pipeline_id,
        program_version,
        &source_checksum,
        &expected_integrity_checksum,
    ));

    let total_size = match save_file(&target_file_path, payload, &expected_integrity_checksum).await
    {
        Ok(total_size) => total_size,
        Err(error) => match unwritable_store_cause(&error) {
            Some(cause) => return Ok(insufficient_storage_response(cause, &error)),
            None => return Err(error),
        },
    };

    info!(
        pipeline_id = %pipeline_id,
        pipeline = "N/A",
        "Successfully received binary (program version: {}) ({} bytes)",
        program_version,
        total_size
    );

    // Return success response
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "message": "Binary uploaded successfully",
        "pipeline_id": pipeline_id.to_string(),
        "program_version": program_version.0,
        "source_checksum": source_checksum,
        "integrity_checksum": expected_integrity_checksum,
        "file_size": total_size
    })))
}

/// Uploads a pipeline's program info using streaming.
/// Metadata is passed via path parameters and the JSON file is streamed directly to disk.
#[post("/program_info/{pipeline_id}/{program_version}/{source_checksum}/{integrity_checksum}")]
async fn upload_program_info(
    config: web::Data<CompilerConfig>,
    req: HttpRequest,
    payload: web::Payload,
) -> Result<impl Responder, ManagerError> {
    // Retrieve URL encoded parameters
    let path_parameters = req.match_info();
    let pipeline_id =
        decode_url_encoded_parameter("pipeline_id", path_parameters.get("pipeline_id"))?;
    let program_version =
        decode_url_encoded_parameter("program_version", path_parameters.get("program_version"))?;
    let source_checksum =
        decode_url_encoded_parameter("source_checksum", path_parameters.get("source_checksum"))?;
    let expected_integrity_checksum = decode_url_encoded_parameter(
        "integrity_checksum",
        path_parameters.get("integrity_checksum"),
    )?;

    // Validate parameters
    let pipeline_id_uuid =
        Uuid::from_str(&pipeline_id).map_err(|e| ApiError::InvalidUuidParam {
            value: pipeline_id.clone(),
            error: e.to_string(),
        })?;
    let pipeline_id = PipelineId(pipeline_id_uuid);

    let program_version =
        Version(
            i64::from_str(&program_version).map_err(|e| ApiError::InvalidVersionParam {
                value: program_version.clone(),
                error: e.to_string(),
            })?,
        );

    validate_is_sha256_checksum(&source_checksum).map_err(|e| {
        ManagerError::from(ApiError::InvalidChecksumParam {
            value: source_checksum.to_string(),
            error: e,
        })
    })?;

    validate_is_sha256_checksum(&expected_integrity_checksum).map_err(|e| {
        ManagerError::from(ApiError::InvalidChecksumParam {
            value: expected_integrity_checksum.to_string(),
            error: e,
        })
    })?;

    // Create pipeline-binaries directory if it doesn't exist
    let pipeline_binaries_dir = config
        .working_dir()
        .join("rust-compilation")
        .join("pipeline-binaries");

    fs::create_dir_all(&pipeline_binaries_dir)
        .await
        .map_err(|e| {
            ManagerError::from(CommonError::io_error(
                format!("creating directory '{}'", pipeline_binaries_dir.display()),
                e,
            ))
        })?;

    // Form the target file path
    let target_file_path = pipeline_binaries_dir.join(program_info_filename(
        &pipeline_id,
        program_version,
        &source_checksum,
        &expected_integrity_checksum,
    ));

    let total_size = match save_file(&target_file_path, payload, &expected_integrity_checksum).await
    {
        Ok(total_size) => total_size,
        Err(error) => match unwritable_store_cause(&error) {
            Some(cause) => return Ok(insufficient_storage_response(cause, &error)),
            None => return Err(error),
        },
    };

    info!(
        pipeline_id = %pipeline_id,
        pipeline = "N/A",
        "Successfully received program info (program version: {}) ({} bytes)",
        program_version,
        total_size
    );

    // Return success response
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "message": "Program info uploaded successfully",
        "pipeline_id": pipeline_id.to_string(),
        "program_version": program_version.0,
        "source_checksum": source_checksum,
        "integrity_checksum": expected_integrity_checksum,
        "file_size": total_size
    })))
}

/// Validates a SQL program by running the SQL compiler without persisting
/// anything or building the Rust binary. Returns the derived schema and
/// connectors, and, when `ir` is set in the request, the program IR.
///
/// Backs the user-facing `/v0/validate_program` endpoint and supplies the IR to
/// the `/diff` endpoint. Always responds with HTTP 200 and a
/// `ValidateProgramResponse`; transport-level failures surface on the caller side.
#[post("/validate_program")]
async fn validate_program_endpoint(
    common_config: web::Data<CommonConfig>,
    config: web::Data<CompilerConfig>,
    body: web::Json<ProgramValidationRequest>,
) -> Result<HttpResponse, ManagerError> {
    let ProgramValidationRequest {
        program_config,
        program_code,
        ir,
    } = body.into_inner();
    let response =
        validate_program(&common_config, &config, &program_config, &program_code, ir).await;
    Ok(HttpResponse::Ok().json(response))
}

/// Streams the payload to a temp file next to `target_file_path`, verifies the
/// sha256 checksum, and only then renames the temp file onto the final path.
/// The final path therefore never holds a partially written or corrupt file;
/// on any error the temp file is removed.
async fn save_file(
    target_file_path: &Path,
    payload: impl Stream<Item = Result<web::Bytes, PayloadError>> + Unpin,
    expected_integrity_checksum: &str,
) -> Result<usize, ManagerError> {
    let temp_file_path = target_file_path.with_added_extension(format!("tmp-{}", Uuid::now_v7()));

    let write_result =
        stream_to_file_and_verify(&temp_file_path, payload, expected_integrity_checksum).await;

    match write_result {
        Ok(total_size) => match fs::rename(&temp_file_path, target_file_path).await {
            Ok(()) => {
                // Persist the rename: without a directory fsync a crash can
                // lose the entry while the database already says Success.
                fsync_parent_dir(target_file_path).await?;
                Ok(total_size)
            }
            Err(e) => {
                remove_temp_upload_file(&temp_file_path).await;
                Err(ManagerError::from(CommonError::io_error(
                    format!(
                        "renaming '{}' to '{}'",
                        temp_file_path.display(),
                        target_file_path.display()
                    ),
                    e,
                )))
            }
        },
        Err(e) => {
            remove_temp_upload_file(&temp_file_path).await;
            Err(e)
        }
    }
}

/// Fsyncs the directory containing `path` so that a rename into it is durable.
async fn fsync_parent_dir(path: &Path) -> Result<(), ManagerError> {
    let Some(parent_dir) = path.parent() else {
        return Ok(());
    };
    let parent_dir_handle = fs::File::open(parent_dir).await.map_err(|e| {
        ManagerError::from(CommonError::io_error(
            format!("opening directory '{}'", parent_dir.display()),
            e,
        ))
    })?;
    parent_dir_handle.sync_all().await.map_err(|e| {
        ManagerError::from(CommonError::io_error(
            format!("syncing directory '{}'", parent_dir.display()),
            e,
        ))
    })
}

/// Streams the payload to `file_path` and validates the sha256 checksum after
/// flushing. Returns the total size in bytes. The caller removes the file on
/// any error.
async fn stream_to_file_and_verify(
    file_path: &Path,
    mut payload: impl Stream<Item = Result<web::Bytes, PayloadError>> + Unpin,
    expected_integrity_checksum: &str,
) -> Result<usize, ManagerError> {
    let mut file = fs::File::create(&file_path).await.map_err(|e| {
        ManagerError::from(CommonError::io_error(
            format!("creating file '{}'", file_path.display()),
            e,
        ))
    })?;

    let mut hasher = aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256);
    let mut total_size = 0usize;

    while let Some(chunk) = payload.next().await {
        let chunk = chunk.map_err(|e| {
            ManagerError::from(CommonError::io_error(
                "reading payload chunk".to_string(),
                std::io::Error::other(format!("Payload error: {e}")),
            ))
        })?;

        // Update checksum
        hasher.update(&chunk);
        total_size += chunk.len();

        // Write chunk to file
        file.write_all(&chunk).await.map_err(|e| {
            ManagerError::from(CommonError::io_error(
                format!("writing to file '{}'", file_path.display()),
                e,
            ))
        })?;
    }

    // Flush and persist to disk before the rename makes the file visible
    // under its final name.
    file.flush().await.map_err(|e| {
        ManagerError::from(CommonError::io_error(
            format!("flushing file '{}'", file_path.display()),
            e,
        ))
    })?;
    file.sync_all().await.map_err(|e| {
        ManagerError::from(CommonError::io_error(
            format!("syncing file '{}'", file_path.display()),
            e,
        ))
    })?;
    drop(file);

    // Validate integrity checksum
    let actual_integrity_checksum = hex::encode(hasher.finish());
    if actual_integrity_checksum != expected_integrity_checksum {
        return Err(ManagerError::from(ApiError::InvalidChecksumParam {
            value: format!(
                "Expected integrity checksum '{}', but calculated '{}'",
                expected_integrity_checksum, actual_integrity_checksum
            ),
            error: "Integrity checksum mismatch".to_string(),
        }));
    }

    Ok(total_size)
}

/// Operator documentation on resolving a full or read-only storage volume.
const OUT_OF_STORAGE_DOCS_URL: &str =
    "https://docs.feldera.com/operations/guide/#out-of-storage-errors";

/// Why the binary store cannot accept writes, phrased as the cause plus the
/// action that resolves it, or `None` when the error is something else. Only
/// ENOSPC and EROFS qualify: both need an operator, so an upload that hits
/// either must fail rather than retry.
fn unwritable_store_cause(error: &ManagerError) -> Option<&'static str> {
    let ManagerError::CommonError {
        common_error: CommonError::IoError { io_error, .. },
    } = error
    else {
        return None;
    };
    match io_error.kind() {
        std::io::ErrorKind::StorageFull => Some(
            "the storage volume is full; an operator must grow it or delete unused pipelines \
             to reclaim space",
        ),
        // EROFS in practice means the underlying disk failed and the kernel
        // remounted the filesystem read-only.
        std::io::ErrorKind::ReadOnlyFilesystem => Some(
            "the storage volume is read-only, which usually means the underlying disk failed; \
             an operator must repair or replace it",
        ),
        _ => None,
    }
}

/// 507 Insufficient Storage response naming the cause and how to resolve it.
/// The distinct status lets workers fail the compilation fast with this
/// message instead of burning their retry budget on a volume that only an
/// operator can grow or repair.
fn insufficient_storage_response(cause: &str, error: &ManagerError) -> HttpResponse {
    HttpResponse::InsufficientStorage().json(serde_json::json!({
        "message": format!(
            "Unable to write to the binary store: {cause}. \
             See {OUT_OF_STORAGE_DOCS_URL}. Underlying error: {error}"
        ),
    }))
}

/// Removes a temp upload file. Failure only leaves an orphan file behind, so
/// it is logged rather than propagated.
async fn remove_temp_upload_file(temp_file_path: &Path) {
    if let Err(e) = fs::remove_file(temp_file_path).await
        && e.kind() != std::io::ErrorKind::NotFound
    {
        warn!(
            "Unable to remove temp upload file '{}': {e}",
            temp_file_path.display()
        );
    }
}

/// Fraction of the working-directory filesystem above which `/healthz` reports
/// storage pressure: binary uploads are about to fail with ENOSPC and an
/// operator must grow the volume or reclaim space.
const STORAGE_PRESSURE_THRESHOLD: f64 = 0.95;

/// Message reported when the working-directory filesystem is at or above
/// `used_fraction_threshold`, `None` when it is below.
fn storage_pressure_message(
    disk_space: &DiskSpace,
    used_fraction_threshold: f64,
) -> Option<String> {
    if disk_space.used_fraction < used_fraction_threshold {
        return None;
    }
    Some(format!(
        "unhealthy: compiler working directory filesystem is {:.1}% full ({} of {} bytes used); \
         binary uploads will fail until an operator grows the volume or storage is reclaimed",
        disk_space.used_fraction * 100.0,
        disk_space.used_byte,
        disk_space.total_byte
    ))
}

/// Query parameters of the `/healthz` endpoint.
#[derive(serde::Deserialize)]
struct HealthzQuery {
    /// Also report storage pressure of the working-directory filesystem.
    #[serde(default)]
    check_storage: bool,
}

/// Health check which returns success if it is able to reach the database.
/// With `?check_storage=true` it also fails when the working-directory
/// filesystem is nearly full.
///
/// Kubernetes probes must omit the parameter, because restarting the pod
/// cannot free disk space and the pod still serves already compiled binaries.
/// The cluster monitor passes it so that /v0/cluster_healthz surfaces the
/// condition to operators, who can act on it.
#[get("/healthz")]
async fn healthz(
    probe: web::Data<Arc<Mutex<DbProbe>>>,
    config: web::Data<CompilerConfig>,
    query: web::Query<HealthzQuery>,
) -> Result<impl Responder, ManagerError> {
    if query.check_storage
        && let Some(disk_space) = DiskSpace::new_from_path(&config.working_dir())
        && let Some(message) = storage_pressure_message(&disk_space, STORAGE_PRESSURE_THRESHOLD)
    {
        return Ok(
            HttpResponse::ServiceUnavailable().json(serde_json::json!({ "status": message }))
        );
    }
    Ok(probe.lock().await.as_http_response())
}

/// Creates the compiler working directory if it does not exist.
async fn create_working_directory_if_not_exists(
    config: &CompilerConfig,
) -> Result<(), ManagerError> {
    if !config.working_dir().exists() {
        fs::create_dir_all(config.working_dir())
            .await
            .map_err(|e| {
                CommonError::io_error(
                    format!("creating directory '{}'", config.working_dir().display()),
                    e,
                )
            })?;
        info!("Compiler server has created a new working directory");
    }

    // Wipe leftover ephemeral (IR-only) compilation directories from a previous
    // run; no compilation is in progress at startup, so any that exist are
    // orphans (see `ephemeral_compilation_dir`).
    let _ = fs::remove_dir_all(ephemeral_compilation_dir(config)).await;

    Ok(())
}

/// Performs SQL and Rust compilation of a dummy program such that subsequent
/// compilations are faster and a `Cargo.lock` has already been generated.
pub async fn compiler_precompile(
    common_config: CommonConfig,
    config: CompilerConfig,
) -> Result<(), ManagerError> {
    // Compiler working directory
    create_working_directory_if_not_exists(&config).await?;

    // Dummy pipeline program values
    let tenant_id = TenantId(Uuid::nil());
    let pipeline_id = PipelineId(Uuid::nil());
    let platform_version = &common_config.platform_version;
    let program_version = Version(1);
    let program_config = ProgramConfig {
        profile: None, // The pre-compilation will use the compiler configuration default profile
        cache: false,
        runtime_version: None,
        use_platform_compiler: false,
    };
    let program_config = serde_json::to_value(&program_config).map_err(|e| {
        CommonError::json_serialization_error(
            "serialize precompile program_config as JSON value".to_string(),
            e,
        )
    })?;
    let program_code = "";
    let udf_rust = "";
    let udf_toml = "";

    // SQL
    let (program_info, sql_duration, _) = perform_sql_compilation(
        &common_config,
        &config,
        None,
        tenant_id,
        pipeline_id,
        None,
        platform_version,
        program_version,
        &program_config,
        program_code,
        udf_rust,
        udf_toml,
        SqlCompilationOutput::Full,
    )
    .await
    .map_err(|e| match e {
        SqlCompilationError::NoLongerExists => CompilerError::PrecompilationError {
            error: "SQL compilation no longer relevant as pipeline no longer exists".to_string(),
        },
        SqlCompilationError::Outdated => CompilerError::PrecompilationError {
            error: "Outdated SQL compilation".to_string(),
        },
        SqlCompilationError::TerminatedBySignal => CompilerError::PrecompilationError {
            error: "SQL compilation terminated by signal".to_string(),
        },
        SqlCompilationError::SqlError(compilation_info) => CompilerError::PrecompilationError {
            error: format!("{:?}", compilation_info),
        },
        SqlCompilationError::SystemError(error) => CompilerError::PrecompilationError { error },
    })?;

    // Rust
    let RustCompilationResult {
        source_checksum,
        binary_integrity_checksum,
        binary_size,
        program_info_integrity_checksum,
        profile,
        duration: rust_duration,
        rustc_result: _rustc_result,
    } = perform_rust_compilation(
        &common_config,
        &config,
        None,
        tenant_id,
        pipeline_id,
        None,
        platform_version,
        program_version,
        &program_config,
        &Some(program_info),
        udf_rust,
        udf_toml,
    )
    .await
    .map_err(|e| match e {
        RustCompilationError::NoLongerExists => CompilerError::PrecompilationError {
            error: "Rust compilation no longer relevant as pipeline no longer exists".to_string(),
        },
        RustCompilationError::Outdated => CompilerError::PrecompilationError {
            error: "Outdated Rust compilation".to_string(),
        },
        RustCompilationError::TerminatedBySignal => CompilerError::PrecompilationError {
            error: "Rust compilation terminated by signal".to_string(),
        },
        RustCompilationError::RustError(compilation_info) => CompilerError::PrecompilationError {
            error: compilation_info.to_string(),
        },
        RustCompilationError::SystemError(error) => CompilerError::PrecompilationError { error },
        RustCompilationError::FileUploadError(error) => {
            CompilerError::PrecompilationError { error }
        }
    })?;

    // Success
    info!(
        "Pre-compilation finished: SQL took {:.2}s and Rust took {:.2}s (source checksum: {}; binary integrity checksum: {}, binary size: {} bytes, program info integrity checksum: {}, profile: {})",
        sql_duration.as_secs_f64(),
        rust_duration.as_secs_f64(),
        source_checksum,
        binary_integrity_checksum,
        binary_size,
        program_info_integrity_checksum,
        profile
    );
    Ok(())
}

/// Main to start the compiler, which consists of:
/// - Thread which does SQL compilation
/// - Thread which does Rust compilation
/// - HTTP server which serves binaries
pub async fn compiler_main(
    common_config: CommonConfig,
    config: CompilerConfig,
    db: Arc<Mutex<StoragePostgres>>,
    worker_id: usize,
    total_workers: usize,
    allow_exit_upon_target_cleared: bool,
) -> Result<(), ManagerError> {
    // All threads will operate in the same working directory.
    // This must be created in advance such that there is no
    // race condition which of the threads creates it first.
    create_working_directory_if_not_exists(&config).await?;

    // Spawn compilation threads
    let sql_task = spawn(sql_compiler_task(
        worker_id,
        total_workers,
        common_config.clone(),
        config.clone(),
        db.clone(),
    ));
    let rust_task = spawn(rust_compiler_task(
        worker_id,
        total_workers,
        common_config.clone(),
        config.clone(),
        db.clone(),
        allow_exit_upon_target_cleared,
    ));

    // Spawn HTTP server thread
    let http_server = spawn_compiler_http_server(&common_config, &config, &db).await;

    // All threads should run indefinitely
    let error = tokio::select! {
        _ = sql_task => "Compiler SQL task ended prematurely",
        _ = rust_task => "Compiler Rust task ended prematurely",
        _ = http_server => "Compiler HTTP(S) server task ended prematurely",
    };
    error!("{error}");
    error!("Returning compiler thread");
    Err(ManagerError::from(CompilerError::TaskFailed {
        error: error.to_string(),
    }))
}

/// Runs the artifact-store variant of the compiler server: the full HTTP
/// surface plus a janitor, but no SQL or Rust compilation tasks. Serves
/// deployments where compiler workers are ephemeral and this process is the
/// durable binary store (see enterprise compiler autoscaling).
pub async fn artifact_server_main(
    common_config: CommonConfig,
    config: CompilerConfig,
    db: Arc<Mutex<StoragePostgres>>,
) -> Result<(), ManagerError> {
    create_working_directory_if_not_exists(&config).await?;

    // Spawn janitor and HTTP server threads
    let janitor_task = spawn(artifact_server_janitor_task(config.clone(), db.clone()));
    let http_server = spawn_compiler_http_server(&common_config, &config, &db).await;

    // Both threads should run indefinitely
    let error = tokio::select! {
        _ = janitor_task => "Artifact server janitor task ended prematurely",
        _ = http_server => "Artifact server HTTP(S) server task ended prematurely",
    };
    error!("{error}");
    Err(ManagerError::from(CompilerError::TaskFailed {
        error: error.to_string(),
    }))
}

/// Spawns the compiler HTTP(S) server serving artifacts, uploads, program
/// validation, and health checks. Panics if the listener cannot be bound.
async fn spawn_compiler_http_server(
    common_config: &CommonConfig,
    config: &CompilerConfig,
    db: &Arc<Mutex<StoragePostgres>>,
) -> JoinHandle<Result<(), std::io::Error>> {
    let config = web::Data::new(config.clone());
    let common_config_data = web::Data::new(common_config.clone());
    let probe = web::Data::new(DbProbe::new(db.clone()).await);
    let server = HttpServer::new(move || {
        actix_web::App::new()
            .app_data(config.clone())
            .app_data(common_config_data.clone())
            .app_data(probe.clone())
            .service(check_compilation_artifacts)
            .service(get_binary)
            .service(get_program_info)
            .service(upload_binary)
            .service(upload_program_info)
            .service(validate_program_endpoint)
            .service(healthz)
    })
    .workers(common_config.http_workers)
    .worker_max_blocking_threads(std::cmp::max(512 / common_config.http_workers, 1));
    let listener = TcpListener::bind((
        common_config.bind_address.clone(),
        common_config.compiler_port,
    ))
    .unwrap_or_else(|_| {
        panic!(
            "compiler unable to bind listener to {}:{} -- is the port occupied?",
            common_config.bind_address, common_config.compiler_port
        )
    });
    let http_server = spawn(
        if let Some(server_config) = common_config.https_server_config() {
            server
                .listen_rustls_0_23(listener, server_config)
                .expect("compiler HTTPS server unable to listen")
                .run()
        } else {
            server
                .listen(listener)
                .expect("compiler HTTP server unable to listen")
                .run()
        },
    );
    info!(
        "Compiler {} server: ready on port {} ({} workers)",
        if common_config.enable_https {
            "HTTPS"
        } else {
            "HTTP"
        },
        common_config.compiler_port,
        common_config.http_workers,
    );
    http_server
}

/// Age above which an ephemeral validation directory is an orphan of a crashed
/// validation; live validations finish within seconds.
const ORPHANED_EPHEMERAL_DIR_MAX_AGE: Duration = Duration::from_secs(3600);

/// Removes an ephemeral validation directory whose modification time exceeds
/// [`ORPHANED_EPHEMERAL_DIR_MAX_AGE`]. Missing or future modification times
/// never remove.
fn decide_orphaned_ephemeral_dir(
    _dir_name: &str,
    metadata: Option<std::fs::Metadata>,
) -> CleanupDecision {
    let Some(modified_time) = metadata.and_then(|metadata| metadata.modified().ok()) else {
        return CleanupDecision::Ignore;
    };
    if modified_time
        .elapsed()
        .is_ok_and(|age| age >= ORPHANED_EPHEMERAL_DIR_MAX_AGE)
    {
        CleanupDecision::Remove
    } else {
        CleanupDecision::Keep {
            motivation: "Validation may still be in progress".to_string(),
        }
    }
}

/// Janitor of the artifact server: garbage-collects pipeline binaries of
/// deleted or recompiled pipelines, ephemeral validation directories orphaned
/// by crashed validations, and stale SQL compiler jars. Errors within a pass
/// are logged and the loop continues.
async fn artifact_server_janitor_task(config: CompilerConfig, db: Arc<Mutex<StoragePostgres>>) {
    loop {
        if let Err(e) = cleanup_pipeline_binaries(&config, db.clone()).await {
            error!("Artifact server janitor: pipeline binaries cleanup failed: {e}");
        }
        let ephemeral_dir = ephemeral_compilation_dir(&config);
        if ephemeral_dir.is_dir()
            && let Err(e) = cleanup_specific_directories(
                "Ephemeral validation directories",
                &ephemeral_dir,
                Arc::new(decide_orphaned_ephemeral_dir),
                false,
                true,
            )
            .await
        {
            error!("Artifact server janitor: ephemeral validation directory cleanup failed: {e}");
        }
        let jar_cache_dir = jar_cache_dir(&config);
        if jar_cache_dir.is_dir()
            && let Err(e) = cleanup_specific_files(
                "SQL JAR cache",
                &jar_cache_dir,
                Arc::new(decide_stale_jar),
                true,
                true,
            )
            .await
        {
            error!("Artifact server janitor: SQL compiler jar cache cleanup failed: {e}");
        }
        sleep(CLEANUP_INTERVAL).await;
    }
}

#[cfg(test)]
mod test {
    use crate::api::error::ApiError;
    use crate::compiler::main::{
        create_working_directory_if_not_exists, decide_orphaned_ephemeral_dir,
        decode_url_encoded_parameter, save_file, upload_binary,
    };
    use crate::compiler::util::CleanupDecision;
    use crate::compiler::util::pipeline_binary_filename;
    use crate::compiler::util::sha256;
    use crate::config::CompilerConfig;
    use crate::db::types::pipeline::PipelineId;
    use crate::db::types::program::CompilationProfile;
    use crate::db::types::version::Version;
    use crate::error::ManagerError;
    use actix_web::error::PayloadError;
    use actix_web::{App, test as actix_test, web};
    use std::time::Duration;
    use tokio::fs;
    use uuid::Uuid;

    #[test]
    fn decoding_url_encoded_parameter() {
        assert!(matches!(
                decode_url_encoded_parameter("example", Some("val1")),
                Ok(s) if s == "val1"
        ));
        assert!(matches!(
            decode_url_encoded_parameter("example", None),
            Err(ManagerError::ApiError {
                api_error: ApiError::MissingUrlEncodedParam {
                    param
                }
            }) if param == "example"
        ));
    }

    #[tokio::test]
    async fn creating_working_directory() {
        // Two directories:
        // - <temp>/existing which is created in advance with a file in it
        // - <temp/non-existing which is not created beforehand
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().to_path_buf();
        let existing_path = path.join("existing");
        fs::create_dir(&existing_path).await.unwrap();
        let existing_file = existing_path.join("example.txt");
        fs::write(&existing_file, "abc".as_bytes()).await.unwrap();
        let non_existing_path = path.join("non-existing");

        // If it already exists, it should not empty it
        assert!(existing_path.is_dir());
        assert!(existing_file.is_file());
        create_working_directory_if_not_exists(&CompilerConfig {
            compiler_working_directory: existing_path.to_string_lossy().to_string(),
            compilation_profile: CompilationProfile::Optimized,
            sql_compiler_path: "".to_string(),
            sql_compiler_cache_url: "".to_string(),
            compilation_cargo_lock_path: "".to_string(),
            dbsp_override_path: "".to_string(),
            binary_upload_endpoint: None,
            binary_upload_timeout_secs: 600,
            binary_upload_max_retries: 3,
            binary_upload_retry_delay_ms: 1000,
            precompile: false,
        })
        .await
        .unwrap();
        assert!(existing_path.is_dir());
        assert!(existing_file.is_file());

        // If it does not exist, it should create a new empty one
        assert!(!non_existing_path.is_dir());
        create_working_directory_if_not_exists(&CompilerConfig {
            compiler_working_directory: non_existing_path.to_string_lossy().to_string(),
            compilation_profile: CompilationProfile::Optimized,
            sql_compiler_path: "".to_string(),
            sql_compiler_cache_url: "".to_string(),
            compilation_cargo_lock_path: "".to_string(),
            dbsp_override_path: "".to_string(),
            binary_upload_endpoint: None,
            binary_upload_timeout_secs: 600,
            binary_upload_max_retries: 3,
            binary_upload_retry_delay_ms: 1000,
            precompile: false,
        })
        .await
        .unwrap();
        assert!(non_existing_path.is_dir());
    }

    #[tokio::test]
    async fn test_streaming_binary_upload_success() {
        let (_tempdir, config) = create_test_config("success");
        let app = create_test_app(config.clone()).await;

        // Test cases with different data sizes
        let test_cases = vec![
            UploadTestCase::new("small", SMALL_TEST_DATA.to_vec()),
            UploadTestCase::new("medium", create_test_data(64 * 1024)), // 64KB
            UploadTestCase::new("large", create_test_data(1024 * 1024)), // 1MB
        ];

        for test_case in test_cases {
            // Send upload request
            let req = actix_test::TestRequest::post()
                .uri(&test_case.url())
                .set_payload(test_case.data.clone())
                .to_request();

            let resp = actix_test::call_service(&app, req).await;

            // Verify successful response
            assert!(
                resp.status().is_success(),
                "Upload should succeed for test case: {}",
                test_case.name
            );

            // Verify response body structure
            let body: serde_json::Value = actix_test::read_body_json(resp).await;
            assert_eq!(body["message"], "Binary uploaded successfully");
            assert_eq!(body["pipeline_id"], test_case.pipeline_id.to_string());
            assert_eq!(body["program_version"], test_case.program_version);
            assert_eq!(body["source_checksum"], test_case.source_checksum);
            assert_eq!(body["integrity_checksum"], test_case.integrity_checksum);
            assert_eq!(body["file_size"], test_case.data.len());

            // Verify file was written correctly
            let expected_path = get_expected_binary_path(
                &std::path::PathBuf::from(&config.compiler_working_directory),
                &test_case.pipeline_id,
                test_case.program_version,
                &test_case.source_checksum,
                &test_case.integrity_checksum,
            );

            assert!(
                expected_path.exists(),
                "Binary file should exist for test case: {}",
                test_case.name
            );

            // Verify file contents match original data
            let written_data = fs::read(&expected_path).await.unwrap();
            assert_eq!(
                written_data, test_case.data,
                "Written data should match original for test case: {}",
                test_case.name
            );

            // Verify integrity checksum
            let actual_checksum = hex::encode(sha256(&written_data));
            assert_eq!(
                actual_checksum, test_case.integrity_checksum,
                "Checksum should match for test case: {}",
                test_case.name
            );

            // No temp file may be left behind
            let dir_names =
                list_file_names(expected_path.parent().expect("path must have a parent")).await;
            assert!(
                dir_names.iter().all(|name| !name.contains(".tmp-")),
                "No temp file may remain, found: {dir_names:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_streaming_binary_upload_checksum_mismatch() {
        let (_tempdir, config) = create_test_config("checksum_fail");
        let app = create_test_app(config.clone()).await;

        let test_data = SMALL_TEST_DATA;
        let pipeline_id = PipelineId(Uuid::now_v7());
        let source_checksum = hex::encode(sha256(b"test_source"));
        let wrong_integrity_checksum = hex::encode(sha256(b"wrong_data")); // Intentionally wrong

        let url = build_upload_url(&pipeline_id, 1, &source_checksum, &wrong_integrity_checksum);

        // Send request with mismatched checksum
        let req = actix_test::TestRequest::post()
            .uri(&url)
            .set_payload(test_data.to_vec())
            .to_request();

        let resp = actix_test::call_service(&app, req).await;

        // Should return 400 Bad Request
        assert_eq!(
            resp.status(),
            400,
            "Upload should fail with checksum mismatch"
        );

        // Verify file was NOT created (should be cleaned up)
        let expected_path = get_expected_binary_path(
            &std::path::PathBuf::from(&config.compiler_working_directory),
            &pipeline_id,
            1,
            &source_checksum,
            &wrong_integrity_checksum,
        );

        assert!(
            !expected_path.exists(),
            "Binary file should not exist after checksum failure"
        );

        // Neither the final file nor a temp file may remain
        let dir_names =
            list_file_names(expected_path.parent().expect("path must have a parent")).await;
        assert!(
            dir_names.is_empty(),
            "No file may remain after checksum failure, found: {dir_names:?}"
        );
    }

    /// An upload whose payload errors mid-stream leaves no file under the
    /// final name and no temp file behind.
    #[tokio::test]
    async fn test_interrupted_upload_leaves_no_file() {
        let tempdir = tempfile::tempdir().unwrap();
        let pipeline_binaries_dir = tempdir.path().join("pipeline-binaries");
        fs::create_dir_all(&pipeline_binaries_dir).await.unwrap();
        let target_file_path = pipeline_binaries_dir.join("pipeline_example_binary");

        let data = b"partial data";
        let interrupted_payload = futures_util::stream::iter(vec![
            Ok(web::Bytes::from_static(data)),
            Err(PayloadError::Incomplete(None)),
        ]);
        let result = save_file(
            &target_file_path,
            interrupted_payload,
            &hex::encode(sha256(data)),
        )
        .await;
        assert!(result.is_err(), "Interrupted upload should fail");

        let dir_names = list_file_names(&pipeline_binaries_dir).await;
        assert!(
            dir_names.is_empty(),
            "No file may remain after an interrupted upload, found: {dir_names:?}"
        );
    }

    /// A full or read-only store yields a cause naming the fix; other I/O
    /// errors yield none.
    #[test]
    fn unwritable_store_cause_detection() {
        let io_error = |kind| {
            ManagerError::from(crate::common_error::CommonError::io_error(
                "writing".to_string(),
                std::io::Error::new(kind, "test"),
            ))
        };
        let full = super::unwritable_store_cause(&io_error(std::io::ErrorKind::StorageFull))
            .expect("a full volume is unwritable");
        assert!(full.contains("full"), "unexpected cause: {full}");
        let read_only =
            super::unwritable_store_cause(&io_error(std::io::ErrorKind::ReadOnlyFilesystem))
                .expect("a read-only volume is unwritable");
        assert!(
            read_only.contains("read-only"),
            "unexpected cause: {read_only}"
        );
        assert!(
            super::unwritable_store_cause(&io_error(std::io::ErrorKind::PermissionDenied))
                .is_none()
        );
    }

    /// The 507 response names the cause and links the operator documentation.
    #[actix_web::test]
    async fn insufficient_storage_response_is_actionable() {
        let error = ManagerError::from(crate::common_error::CommonError::io_error(
            "writing".to_string(),
            std::io::Error::new(std::io::ErrorKind::StorageFull, "test"),
        ));
        let cause = super::unwritable_store_cause(&error).unwrap();
        let response = super::insufficient_storage_response(cause, &error);
        assert_eq!(
            response.status(),
            actix_web::http::StatusCode::INSUFFICIENT_STORAGE
        );
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        let message = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["message"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(
            message.starts_with("Unable to write to the binary store:"),
            "unexpected message: {message}"
        );
        assert!(message.contains(super::OUT_OF_STORAGE_DOCS_URL));
    }

    /// The kernel errnos the upload path actually sees map to the
    /// `ErrorKind`s `unwritable_store_cause` matches on.
    #[cfg(unix)]
    #[test]
    fn unwritable_store_errnos_map_to_expected_error_kinds() {
        assert_eq!(
            std::io::Error::from_raw_os_error(libc::ENOSPC).kind(),
            std::io::ErrorKind::StorageFull
        );
        assert_eq!(
            std::io::Error::from_raw_os_error(libc::EROFS).kind(),
            std::io::ErrorKind::ReadOnlyFilesystem
        );
    }

    /// Storage pressure is reported at or above the threshold and stays silent
    /// below it.
    #[test]
    fn storage_pressure_threshold() {
        let disk_space = |used_fraction: f64| crate::compiler::util::DiskSpace {
            total_byte: 100,
            used_byte: (used_fraction * 100.0) as u64,
            used_fraction,
            available_byte: 100 - (used_fraction * 100.0) as u64,
            available_fraction: 1.0 - used_fraction,
        };
        assert!(super::storage_pressure_message(&disk_space(0.5), 0.95).is_none());
        assert!(super::storage_pressure_message(&disk_space(0.95), 0.95).is_some());
        let message = super::storage_pressure_message(&disk_space(1.0), 0.95).unwrap();
        assert!(message.contains("100.0% full"));
    }

    /// An old ephemeral validation directory is removed, a fresh one is kept,
    /// and missing metadata never removes.
    #[test]
    fn orphaned_ephemeral_dir_decision() {
        let tempdir = tempfile::tempdir().unwrap();
        let dir_path = tempdir.path();
        let recent = std::fs::metadata(dir_path).unwrap();
        assert!(matches!(
            decide_orphaned_ephemeral_dir("d", Some(recent)),
            CleanupDecision::Keep { .. }
        ));
        let old_time = std::time::SystemTime::now() - Duration::from_secs(2 * 3600);
        let times = std::fs::FileTimes::new()
            .set_accessed(old_time)
            .set_modified(old_time);
        std::fs::File::open(dir_path)
            .unwrap()
            .set_times(times)
            .unwrap();
        let old = std::fs::metadata(dir_path).unwrap();
        assert_eq!(
            decide_orphaned_ephemeral_dir("d", Some(old)),
            CleanupDecision::Remove
        );
        assert_eq!(
            decide_orphaned_ephemeral_dir("d", None),
            CleanupDecision::Ignore
        );
    }

    #[tokio::test]
    async fn test_streaming_binary_upload_invalid_parameters() {
        let (_tempdir, config) = create_test_config("invalid_params");
        let app = create_test_app(config).await;

        let test_data = SMALL_TEST_DATA;
        let valid_pipeline_id = Uuid::now_v7();

        // Test cases for parameter validation
        let invalid_cases = vec![
            (
                "invalid UUID",
                "/binary/not-a-uuid/1/".to_string() + VALID_SHA256 + "/" + VALID_SHA256,
            ),
            (
                "invalid version",
                format!(
                    "/binary/{}/not-a-number/{}/{}",
                    valid_pipeline_id, VALID_SHA256, VALID_SHA256
                ),
            ),
            (
                "invalid source checksum",
                format!("/binary/{}/1/short/{}", valid_pipeline_id, VALID_SHA256),
            ),
            (
                "invalid integrity checksum",
                format!("/binary/{}/1/{}/short", valid_pipeline_id, VALID_SHA256),
            ),
        ];

        for (test_name, url) in invalid_cases {
            let req = actix_test::TestRequest::post()
                .uri(&url)
                .set_payload(test_data.to_vec())
                .to_request();

            let resp = actix_test::call_service(&app, req).await;

            assert_eq!(
                resp.status(),
                400,
                "Should return 400 Bad Request for test case: {}",
                test_name
            );
        }
    }

    // Test helper functions and constants
    /// A Gen-2 pipeline delivers no binary, so the artifact check passes "none"
    /// for the binary integrity checksum. The endpoint must then verify only the
    /// program info: an absent binary is expected, not a missing artifact.
    #[actix_web::test]
    async fn artifacts_present_for_gen2_without_binary() {
        use crate::compiler::util::program_info_filename;

        let (_tempdir, config) = create_test_config("gen2_artifacts");
        let pipeline_id = PipelineId(Uuid::now_v7());
        let program_version = 1i64;
        // The Gen-2 engine names the program info artifact by its own integrity checksum and
        // reuses that value as the source checksum.
        let checksum = hex::encode(sha256(b"gen2-program-info"));

        let app = actix_test::init_service(
            App::new()
                .app_data(web::Data::new(config.clone()))
                .service(super::check_compilation_artifacts),
        )
        .await;
        let url = format!("/artifacts/{pipeline_id}/{program_version}/{checksum}/none/{checksum}");

        // Program info not yet delivered: reported missing (drives a recompile),
        // and not an error merely because there is no binary.
        let resp =
            actix_test::call_service(&app, actix_test::TestRequest::get().uri(&url).to_request())
                .await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);
        let body: serde_json::Value = actix_test::read_body_json(resp).await;
        assert_eq!(body["binary_exists"], serde_json::json!(false));
        assert_eq!(body["program_info_exists"], serde_json::json!(false));

        // Deliver the program info where the endpoint serves it from.
        let pipeline_binaries_dir = std::path::PathBuf::from(&config.compiler_working_directory)
            .join("rust-compilation")
            .join("pipeline-binaries");
        fs::create_dir_all(&pipeline_binaries_dir).await.unwrap();
        fs::write(
            pipeline_binaries_dir.join(program_info_filename(
                &pipeline_id,
                Version(program_version),
                &checksum,
                &checksum,
            )),
            b"{}",
        )
        .await
        .unwrap();

        // Program info present and no binary expected: artifacts are present even
        // though binary_exists is false.
        let resp =
            actix_test::call_service(&app, actix_test::TestRequest::get().uri(&url).to_request())
                .await;
        assert_eq!(
            resp.status(),
            actix_web::http::StatusCode::OK,
            "Gen-2 artifacts are present with program info delivered and no binary"
        );
    }

    const SMALL_TEST_DATA: &[u8] = b"Hello, World! This is test binary data.";
    const VALID_SHA256: &str = "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234";

    /// Creates a test CompilerConfig with a temporary directory
    fn create_test_config(test_name: &str) -> (tempfile::TempDir, CompilerConfig) {
        let tempdir = tempfile::tempdir().unwrap();
        let working_dir = tempdir.path().join(test_name);

        let config = CompilerConfig {
            compiler_working_directory: working_dir.to_string_lossy().to_string(),
            compilation_profile: CompilationProfile::Optimized,
            sql_compiler_path: String::new(),
            sql_compiler_cache_url: String::new(),
            compilation_cargo_lock_path: String::new(),
            dbsp_override_path: String::new(),
            binary_upload_endpoint: None,
            binary_upload_timeout_secs: 600,
            binary_upload_max_retries: 3,
            binary_upload_retry_delay_ms: 1000,
            precompile: false,
        };

        (tempdir, config)
    }

    /// Creates a test app with the upload_binary service
    async fn create_test_app(
        config: CompilerConfig,
    ) -> impl actix_web::dev::Service<
        actix_http::Request,
        Response = actix_web::dev::ServiceResponse,
        Error = actix_web::Error,
    > {
        actix_test::init_service(
            App::new()
                .app_data(web::Data::new(config))
                .service(upload_binary),
        )
        .await
    }

    /// Creates test binary data of specified size with predictable pattern
    fn create_test_data(size: usize) -> Vec<u8> {
        (0..size).map(|i| (i % 256) as u8).collect()
    }

    /// Lists the file names in a directory
    async fn list_file_names(dir: &std::path::Path) -> Vec<String> {
        let mut file_names = vec![];
        let mut entries = fs::read_dir(dir).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            file_names.push(entry.file_name().to_string_lossy().to_string());
        }
        file_names
    }

    /// Builds upload URL from parameters
    fn build_upload_url(
        pipeline_id: &PipelineId,
        program_version: i64,
        source_checksum: &str,
        integrity_checksum: &str,
    ) -> String {
        format!(
            "/binary/{}/{}/{}/{}",
            pipeline_id, program_version, source_checksum, integrity_checksum
        )
    }

    /// Creates expected file path for a binary
    fn get_expected_binary_path(
        working_dir: &std::path::Path,
        pipeline_id: &PipelineId,
        program_version: i64,
        source_checksum: &str,
        integrity_checksum: &str,
    ) -> std::path::PathBuf {
        working_dir
            .join("rust-compilation")
            .join("pipeline-binaries")
            .join(pipeline_binary_filename(
                pipeline_id,
                Version(program_version),
                source_checksum,
                integrity_checksum,
            ))
    }

    /// Test data structure for parameterized tests
    struct UploadTestCase {
        name: &'static str,
        data: Vec<u8>,
        pipeline_id: PipelineId,
        program_version: i64,
        source_checksum: String,
        integrity_checksum: String,
    }

    impl UploadTestCase {
        fn new(name: &'static str, data: Vec<u8>) -> Self {
            let source_checksum = hex::encode(sha256(b"test_source"));
            let integrity_checksum = hex::encode(sha256(&data));

            Self {
                name,
                data,
                pipeline_id: PipelineId(Uuid::now_v7()),
                program_version: 1,
                source_checksum,
                integrity_checksum,
            }
        }

        fn url(&self) -> String {
            build_upload_url(
                &self.pipeline_id,
                self.program_version,
                &self.source_checksum,
                &self.integrity_checksum,
            )
        }
    }
}
