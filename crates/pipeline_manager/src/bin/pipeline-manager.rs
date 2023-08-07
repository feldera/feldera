use actix_web::rt::{self, spawn};
use clap::{Args, Command, FromArgMatches};

use colored::Colorize;
use pipeline_manager::api::ApiDoc;
use pipeline_manager::compiler::Compiler;
use pipeline_manager::config::{CompilerConfig, DatabaseConfig, LocalRunnerConfig, ManagerConfig};
use pipeline_manager::db::ProjectDB;
use pipeline_manager::runner::LocalRunner;
use std::sync::Arc;
use tokio::sync::Mutex;
use utoipa::OpenApi;

// Standalone binary that runs the pipeline manager, compiler and local runner services.
fn main() -> anyhow::Result<()> {
    // Stay in single-threaded mode (no tokio) until calling `daemonize`.

    // Create env logger.
    let name = "[manager]".cyan();
    pipeline_manager::logging::init_logging(name);

    let cli = Command::new("Pipeline manager CLI");
    let cli = DatabaseConfig::augment_args(cli);
    let cli = ManagerConfig::augment_args(cli);
    let cli = CompilerConfig::augment_args(cli);
    let cli = LocalRunnerConfig::augment_args(cli);
    let matches = cli.get_matches();

    let mut manager_config = ManagerConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    if manager_config.dump_openapi {
        let openapi_json = ApiDoc::openapi().to_json()?;
        std::fs::write("openapi.json", openapi_json.as_bytes())?;
        return Ok(());
    }

    if let Some(config_file) = &manager_config.config_file {
        let config_yaml = std::fs::read(config_file).map_err(|e| {
            anyhow::Error::msg(format!("error reading config file '{config_file}': {e}"))
        })?;
        let config_yaml = String::from_utf8_lossy(&config_yaml);
        manager_config = serde_yaml::from_str(&config_yaml).map_err(|e| {
            anyhow::Error::msg(format!("error parsing config file '{config_file}': {e}"))
        })?;
    }
    let compiler_config = CompilerConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    let local_runner_config = LocalRunnerConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();

    let manager_config = manager_config.canonicalize()?;
    let compiler_config = compiler_config.canonicalize()?;
    let local_runner_config = local_runner_config.canonicalize()?;
    if compiler_config.precompile {
        actix_web::rt::System::new()
            .block_on(Compiler::precompile_dependencies(&compiler_config))?;
        return Ok(());
    }
    let database_config = DatabaseConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    let listener = pipeline_manager::api::create_listener(manager_config.clone())?;
    rt::System::new().block_on(async move {
        let db: ProjectDB = ProjectDB::connect(
            &database_config,
            #[cfg(feature = "pg-embed")]
            Some(&manager_config),
        )
        .await
        .unwrap();
        let db = Arc::new(Mutex::new(db));
        let db_clone = db.clone();
        let _compiler = spawn(async move {
            Compiler::run(&compiler_config.clone(), db_clone)
                .await
                .unwrap();
        });
        let db_clone = db.clone();
        let _local_runner = spawn(async move {
            LocalRunner::run(db_clone, &local_runner_config.clone()).await;
        });
        // The api-server blocks forever
        pipeline_manager::api::run(listener, db, manager_config)
            .await
            .unwrap();
    });
    Ok(())
}
