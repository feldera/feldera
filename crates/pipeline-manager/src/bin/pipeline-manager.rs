/// A binary that brings up all three of the api-server, compiler and local
/// runner services.
use clap::{Args, Command, FromArgMatches};

use colored::Colorize;
use pipeline_manager::api::main::ApiDoc;
use pipeline_manager::cluster_health::regular_health_check;
use pipeline_manager::compiler::main::{compiler_main, compiler_precompile};
#[cfg(feature = "postgresql_embedded")]
use pipeline_manager::config::PgEmbedConfig;
use pipeline_manager::config::{
    ApiServerConfig, CommonConfig, CompilerConfig, DatabaseConfig, LocalRunnerConfig,
};
use pipeline_manager::db::storage_postgres::StoragePostgres;
use pipeline_manager::runner::local_runner::LocalRunner;
use pipeline_manager::runner::main::runner_main;
use pipeline_manager::{ensure_default_crypto_provider, init_fd_limit, platform_enable_unstable};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::info;
use utoipa::OpenApi;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    ensure_default_crypto_provider();
    init_fd_limit();

    // Create env logger.
    let name = "[manager]".cyan();
    pipeline_manager::logging::init_logging(name);

    let cli = Command::new("Pipeline manager CLI");
    let cli = CommonConfig::augment_args(cli);
    #[cfg(feature = "postgresql_embedded")]
    let cli = PgEmbedConfig::augment_args(cli);
    let cli = DatabaseConfig::augment_args(cli);
    let cli = ApiServerConfig::augment_args(cli);
    let cli = CompilerConfig::augment_args(cli);
    let cli = LocalRunnerConfig::augment_args(cli);
    let matches = cli.get_matches();
    let common_config = CommonConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    if let Some(features) = &common_config.unstable_features {
        platform_enable_unstable(features);
    }
    #[cfg(feature = "postgresql_embedded")]
    let pg_embed_config = PgEmbedConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    let api_config = ApiServerConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    if api_config.dump_openapi {
        let openapi_json = ApiDoc::openapi().to_pretty_json()?;
        tokio::fs::write("openapi.json", openapi_json.as_bytes()).await?;
        return Ok(());
    }
    let compiler_config = CompilerConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    let local_runner_config = LocalRunnerConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();

    let common_config = common_config.canonicalize()?;
    #[cfg(feature = "postgresql_embedded")]
    let pg_embed_config = pg_embed_config.canonicalize()?;
    // `api_config` currently does not have any paths
    let compiler_config = compiler_config.canonicalize()?;
    let local_runner_config = local_runner_config.canonicalize()?;

    if compiler_config.precompile {
        compiler_precompile(common_config, compiler_config).await?;
        info!("Pre-compilation finished");
        return Ok(());
    }
    let database_config = DatabaseConfig::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();
    let db: StoragePostgres = StoragePostgres::connect(
        &database_config,
        #[cfg(feature = "postgresql_embedded")]
        pg_embed_config.clone(),
    )
    .await
    .expect("Could not open connection to database");

    // Run migrations before starting any service
    db.run_migrations().await?;
    let db = Arc::new(Mutex::new(db));

    let db_clone = db.clone();
    let common_config_clone = common_config.clone();
    let compiler_config_clone = compiler_config.clone();

    // Running multiple compiler workers on the same machine is not yet supported
    // due to the HTTP server ran by compiler_main & cleanup status files.
    // For now, we run a single compiler worker in the pipeline manager.
    let worker_id = 0;
    let total_workers = 1;
    let _compiler = tokio::spawn(async move {
        compiler_main(
            common_config_clone,
            compiler_config_clone,
            db_clone,
            worker_id,
            total_workers,
        )
        .await
        .expect("Compiler server main failed");
    });

    let db_clone = db.clone();
    let common_config_clone = common_config.clone();
    let _local_runner = tokio::spawn(async move {
        runner_main::<LocalRunner>(db_clone, common_config_clone, local_runner_config.clone())
            .await;
    });

    let health_check = Arc::new(RwLock::new(None));
    let health_check_handle = health_check.clone();
    let common_config_clone = common_config.clone();
    tokio::spawn(async move {
        regular_health_check(health_check_handle, common_config_clone, None).await;
    });

    // The api-server blocks forever
    pipeline_manager::api::main::run(
        db,
        common_config,
        api_config,
        Arc::new(RwLock::new(None)),
        health_check,
    )
    .await
    .expect("API server main failed");
    Ok(())
}
