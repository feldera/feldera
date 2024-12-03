use crate::api::error::ApiError;
use crate::compiler::error::CompilerError;
use crate::compiler::rust_compiler::{
    perform_rust_compilation, rust_compiler_task, RustCompilationError,
};
use crate::compiler::sql_compiler::{
    perform_sql_compilation, sql_compiler_task, SqlCompilationError,
};
use crate::compiler::util::recreate_dir;
use crate::config::{CommonConfig, CompilerConfig};
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::common::Version;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::program::ProgramConfig;
use crate::db::types::tenant::TenantId;
use crate::error::ManagerError;
use crate::probe::Probe;
use actix_files::NamedFile;
use actix_web::{get, web, HttpRequest, HttpServer, Responder};
use futures_util::join;
use log::{error, info};
use std::str::FromStr;
use std::sync::Arc;
use tokio::{spawn, sync::Mutex};
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
        Some(checksum) => Ok(checksum.to_string()),
    }
}

/// Validates the provided string is a hex-encoded SHA256 checksum.
fn validate_is_sha256_checksum(s: &str) -> Result<(), ManagerError> {
    if s.len() != 64 {
        return Err(ApiError::InvalidChecksumParam {
            value: s.to_string(),
            error: format!("{} characters long instead of expected 64", s.len()),
        }
        .into());
    };
    for c in s.chars() {
        if !c.is_ascii_digit()
            && c != 'a'
            && c != 'b'
            && c != 'c'
            && c != 'd'
            && c != 'e'
            && c != 'f'
        {
            return Err(ApiError::InvalidChecksumParam {
                value: s.to_string(),
                error: format!("character '{c}' is not hexadecimal (0-9, a-f)"),
            }
            .into());
        }
    }
    Ok(())
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
    validate_is_sha256_checksum(&source_checksum)?;
    validate_is_sha256_checksum(&integrity_checksum)?;

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
async fn healthz(probe: web::Data<Arc<Mutex<Probe>>>) -> Result<impl Responder, ManagerError> {
    probe.lock().await.status_as_http_response()
}

/// Creates the compiler working directory if it does not exist.
async fn create_working_directory_if_not_exists(
    config: &CompilerConfig,
) -> Result<(), ManagerError> {
    if !config.working_dir().exists() {
        info!("Compiler server has created a new working directory");
        recreate_dir(&config.working_dir()).await?;
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
    };
    let program_code = "";
    let udf_rust = "";
    let udf_toml = "";

    // SQL
    let (program_info, sql_duration) = perform_sql_compilation(
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
        SqlCompilationError::Outdated => CompilerError::PrecompilationError {
            error: "Outdated SQL compilation".to_string(),
        },
        SqlCompilationError::TerminatedBySignal => CompilerError::PrecompilationError {
            error: "SQL compilation terminated by signal".to_string(),
        },
        SqlCompilationError::SqlError(messages) => CompilerError::PrecompilationError {
            error: format!("{:?}", messages),
        },
        SqlCompilationError::SystemError(error) => CompilerError::PrecompilationError { error },
    })?;

    // Rust
    let (_, source_checksum, integrity_checksum, rust_duration, _) = perform_rust_compilation(
        &common_config,
        &config,
        None,
        tenant_id,
        pipeline_id,
        platform_version,
        program_version,
        &program_config,
        &program_info.main_rust,
        &program_info.udf_stubs,
        udf_rust,
        udf_toml,
    )
    .await
    .map_err(|e| match e {
        RustCompilationError::Outdated => CompilerError::PrecompilationError {
            error: "Outdated Rust compilation".to_string(),
        },
        RustCompilationError::TerminatedBySignal => CompilerError::PrecompilationError {
            error: "Rust compilation terminated by signal".to_string(),
        },
        RustCompilationError::RustError(error) => CompilerError::PrecompilationError { error },
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
    let port = config.binary_ref_port;
    let config = web::Data::new(config.clone());
    let probe = web::Data::new(Probe::new(db.clone()).await);
    let http_server = spawn(
        HttpServer::new(move || {
            actix_web::App::new()
                .app_data(config.clone())
                .app_data(probe.clone())
                .service(get_binary)
                .service(healthz)
        })
        .bind(("0.0.0.0", port))
        .unwrap_or_else(|_| panic!("Unable to bind compiler HTTP server on port {port}"))
        .run(),
    );

    // All threads should run indefinitely
    let _ = join!(sql_task, rust_task, http_server);
    error!("Compiler task threads all exited unexpectedly");
    Ok(())
}
