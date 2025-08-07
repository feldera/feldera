use crate::api::error::ApiError;
use crate::common_error::CommonError;
use crate::compiler::error::CompilerError;
use crate::compiler::rust_compiler::{
    perform_rust_compilation, rust_compiler_task, RustCompilationError,
};
use crate::compiler::sql_compiler::{
    perform_sql_compilation, sql_compiler_task, SqlCompilationError,
};
use crate::compiler::util::validate_is_sha256_checksum;
use crate::config::{CommonConfig, CompilerConfig};
use crate::db::probe::DbProbe;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::program::ProgramConfig;
use crate::db::types::tenant::TenantId;
use crate::db::types::version::Version;
use crate::error::ManagerError;
use actix_files::NamedFile;
use actix_web::{get, web, HttpRequest, HttpServer, Responder};
use futures_util::join;
use log::{error, info};
use std::net::TcpListener;
use std::str::FromStr;
use std::sync::Arc;
use tokio::{fs, spawn, sync::Mutex};
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
        .join(format!(
            "pipeline_{pipeline_id}_v{program_version}_sc_{source_checksum}_ic_{integrity_checksum}"
        ));

    // Read and return file as response
    Ok(NamedFile::open_async(binary_file_path).await)
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
    let (source_checksum, integrity_checksum, rust_duration, _) = perform_rust_compilation(
        &common_config,
        &config,
        None,
        tenant_id,
        pipeline_id,
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
    })?;

    // Success
    info!(
        "Pre-compilation finished: SQL took {:.2}s and Rust took {:.2}s (source checksum: {}; integrity checksum: {})",
        sql_duration.as_secs_f64(),
        rust_duration.as_secs_f64(),
        source_checksum,
        integrity_checksum,
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
) -> Result<(), ManagerError> {
    // All threads will operate in the same working directory.
    // This must be created in advance such that there is no
    // race condition which of the threads creates it first.
    create_working_directory_if_not_exists(&config).await?;

    // Spawn compilation threads
    let sql_task = spawn(sql_compiler_task(
        common_config.clone(),
        config.clone(),
        db.clone(),
    ));
    let rust_task = spawn(rust_compiler_task(
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
        create_working_directory_if_not_exists, decode_url_encoded_parameter,
    };
    use crate::config::CompilerConfig;
    use crate::db::types::program::CompilationProfile;
    use crate::error::ManagerError;
    use tokio::fs;

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
            precompile: false,
        })
        .await
        .unwrap();
        assert!(non_existing_path.is_dir());
    }
}
