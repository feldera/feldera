use crate::api::error::ApiError;
use crate::common_error::CommonError;
use crate::compiler::error::CompilerError;
use crate::compiler::rust_compiler::{
    perform_rust_compilation, rust_compiler_task, RustCompilationError, RustCompilationResult,
};
use crate::compiler::sql_compiler::{
    perform_sql_compilation, sql_compiler_task, SqlCompilationError,
};
use crate::compiler::util::{
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
use actix_web::{get, post, web, HttpRequest, HttpResponse, HttpServer, Responder};
use futures_util::{join, StreamExt};
use std::net::TcpListener;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use tokio::{fs, io::AsyncWriteExt, spawn, sync::Mutex};
use tracing::{error, info};
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

    let total_size = save_file(&target_file_path, payload, &expected_integrity_checksum).await?;

    info!(
        "Successfully received binary for pipeline {} (program version: {}) ({} bytes)",
        pipeline_id, program_version, total_size
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

    let total_size = save_file(&target_file_path, payload, &expected_integrity_checksum).await?;

    info!(
        "Successfully received program info for pipeline {} (program version: {}) ({} bytes)",
        pipeline_id, program_version, total_size
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

async fn save_file(
    target_file_path: &Path,
    mut payload: web::Payload,
    expected_integrity_checksum: &str,
) -> Result<usize, ManagerError> {
    // Stream the binary directly to disk with integrity checksum validation
    let mut file = fs::File::create(&target_file_path).await.map_err(|e| {
        ManagerError::from(CommonError::io_error(
            format!("creating file '{}'", target_file_path.display()),
            e,
        ))
    })?;

    let mut hasher = openssl::sha::Sha256::new();
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
                format!("writing to file '{}'", target_file_path.display()),
                e,
            ))
        })?;
    }

    // Flush and close file
    file.flush().await.map_err(|e| {
        ManagerError::from(CommonError::io_error(
            format!("flushing file '{}'", target_file_path.display()),
            e,
        ))
    })?;
    drop(file);

    // Validate integrity checksum
    let actual_integrity_checksum = hex::encode(hasher.finish());
    if actual_integrity_checksum != expected_integrity_checksum {
        // Remove the invalid file
        let _ = fs::remove_file(&target_file_path).await;
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

/// Health check which returns success if it is able to reach the database.
#[get("/healthz")]
async fn healthz(probe: web::Data<Arc<Mutex<DbProbe>>>) -> Result<impl Responder, ManagerError> {
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
    ));

    // Spawn HTTP server thread
    let config = web::Data::new(config.clone());
    let probe = web::Data::new(DbProbe::new(db.clone()).await);
    let server = HttpServer::new(move || {
        actix_web::App::new()
            .app_data(config.clone())
            .app_data(probe.clone())
            .service(get_binary)
            .service(get_program_info)
            .service(upload_binary)
            .service(upload_program_info)
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

    // All threads should run indefinitely
    let _ = join!(sql_task, rust_task, http_server);
    error!("Compiler task threads all exited unexpectedly");
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::api::error::ApiError;
    use crate::compiler::main::{
        create_working_directory_if_not_exists, decode_url_encoded_parameter, upload_binary,
    };
    use crate::compiler::util::pipeline_binary_filename;
    use crate::config::CompilerConfig;
    use crate::db::types::pipeline::PipelineId;
    use crate::db::types::program::CompilationProfile;
    use crate::db::types::version::Version;
    use crate::error::ManagerError;
    use actix_web::{test as actix_test, web, App};
    use openssl::sha::sha256;
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
